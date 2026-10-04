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

//! File operations admitted before acquiring native descriptors.
//!
//! Native handles are private: cloning, directory iteration, convenience reads,
//! and temporary operations must retain their permit for the full handle lifetime.

use crate::file_descriptors::{
    context_for_path, DescriptorKind, DescriptorPermit, FileOpenContext,
};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;

pub use std::fs::{FileType, Metadata, Permissions, TryLockError};

/// A mutable native handle or an immutable logical reference. Logical files
/// retain their object identity and cursor without retaining a native handle.
#[derive(Debug)]
pub struct File {
    backing: FileBacking,
}

#[derive(Debug)]
enum FileBacking {
    Native(NativeFile),
    Immutable(Box<ImmutableFile>),
}

#[derive(Debug)]
struct NativeFile {
    // Declaration order closes the native handle before returning its capacity.
    inner: std::fs::File,
    permit: DescriptorPermit,
}

#[derive(Debug, Clone)]
struct ImmutableFile {
    binding: crate::immutable_files::ImmutableFileBinding,
    handles: Arc<crate::immutable_files::ImmutableFileHandles>,
    context: FileOpenContext,
    // Match File::try_clone: clones share the logical sequential cursor. All
    // positioned reads bypass it and retain a cache lease for the entire read.
    cursor: Arc<std::sync::Mutex<u64>>,
}

impl File {
    pub fn options() -> OpenOptions {
        OpenOptions::new()
    }

    pub fn open(path: impl AsRef<Path>) -> io::Result<Self> {
        OpenOptions::new().read(true).open(path)
    }

    pub fn create(path: impl AsRef<Path>) -> io::Result<Self> {
        OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(path)
    }

    pub fn try_clone(&self) -> io::Result<Self> {
        let backing = match &self.backing {
            FileBacking::Immutable(file) => FileBacking::Immutable(file.clone()),
            FileBacking::Native(file) => {
                let permit = file.permit.context.acquire(file.permit.kind())?;
                let inner = file
                    .inner
                    .try_clone()
                    .map_err(|error| file.permit.context.map_open_error(error))?;
                FileBacking::Native(NativeFile { inner, permit })
            }
        };
        Ok(Self { backing })
    }

    pub(crate) fn with_native<T>(
        &self,
        operation: impl FnOnce(&std::fs::File) -> io::Result<T>,
    ) -> io::Result<T> {
        match &self.backing {
            FileBacking::Native(file) => operation(&file.inner),
            FileBacking::Immutable(file) => {
                let lease = file.handles.get(&file.binding, &file.context)?;
                lease.with_native(operation)
            }
        }
    }

    fn writable_native(&self) -> io::Result<&std::fs::File> {
        match &self.backing {
            FileBacking::Native(file) => Ok(&file.inner),
            FileBacking::Immutable(_) => Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "immutable logical file cannot be modified",
            )),
        }
    }

    pub fn metadata(&self) -> io::Result<Metadata> {
        self.with_native(std::fs::File::metadata)
    }
    pub fn sync_all(&self) -> io::Result<()> {
        self.with_native(std::fs::File::sync_all)
    }
    pub fn sync_data(&self) -> io::Result<()> {
        self.with_native(std::fs::File::sync_data)
    }
    pub fn set_len(&self, size: u64) -> io::Result<()> {
        self.writable_native()?.set_len(size)
    }
    pub fn set_permissions(&self, permissions: Permissions) -> io::Result<()> {
        self.writable_native()?.set_permissions(permissions)
    }
    pub fn lock(&self) -> io::Result<()> {
        self.writable_native()?.lock()
    }
    pub fn try_lock(&self) -> Result<(), TryLockError> {
        self.writable_native()
            .map_err(TryLockError::Error)?
            .try_lock()
    }
    pub fn unlock(&self) -> io::Result<()> {
        self.writable_native()?.unlock()
    }

    fn read_sequential(&self, buffer: &mut [u8]) -> io::Result<usize> {
        match &self.backing {
            FileBacking::Native(file) => (&file.inner).read(buffer),
            FileBacking::Immutable(file) => {
                let mut cursor = file
                    .cursor
                    .lock()
                    .unwrap_or_else(|error| error.into_inner());
                let available = file.binding.reference.byte_length.saturating_sub(*cursor);
                let length = buffer
                    .len()
                    .min(usize::try_from(available).unwrap_or(usize::MAX));
                if length == 0 {
                    return Ok(0);
                }
                crate::io::read_exact_at(self, &mut buffer[..length], *cursor)?;
                *cursor += length as u64;
                Ok(length)
            }
        }
    }

    fn read_sequential_vectored(&self, buffers: &mut [io::IoSliceMut<'_>]) -> io::Result<usize> {
        match &self.backing {
            FileBacking::Native(file) => (&file.inner).read_vectored(buffers),
            FileBacking::Immutable(_) => match buffers.iter_mut().find(|buffer| !buffer.is_empty())
            {
                Some(buffer) => self.read_sequential(buffer),
                None => Ok(0),
            },
        }
    }

    fn seek_sequential(&self, position: SeekFrom) -> io::Result<u64> {
        match &self.backing {
            FileBacking::Native(file) => (&file.inner).seek(position),
            FileBacking::Immutable(file) => {
                let mut cursor = file
                    .cursor
                    .lock()
                    .unwrap_or_else(|error| error.into_inner());
                let next = match position {
                    SeekFrom::Start(offset) => i128::from(offset),
                    SeekFrom::Current(offset) => i128::from(*cursor) + i128::from(offset),
                    SeekFrom::End(offset) => {
                        i128::from(file.binding.reference.byte_length) + i128::from(offset)
                    }
                };
                *cursor = u64::try_from(next).map_err(|_| {
                    io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "immutable file seek is outside the representable range",
                    )
                })?;
                Ok(*cursor)
            }
        }
    }
}

impl Read for File {
    fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
        self.read_sequential(buffer)
    }
    fn read_vectored(&mut self, buffers: &mut [io::IoSliceMut<'_>]) -> io::Result<usize> {
        self.read_sequential_vectored(buffers)
    }
}
impl Read for &File {
    fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
        self.read_sequential(buffer)
    }
    fn read_vectored(&mut self, buffers: &mut [io::IoSliceMut<'_>]) -> io::Result<usize> {
        self.read_sequential_vectored(buffers)
    }
}
impl Write for File {
    fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
        self.writable_native()?.write(buffer)
    }
    fn write_vectored(&mut self, buffers: &[io::IoSlice<'_>]) -> io::Result<usize> {
        self.writable_native()?.write_vectored(buffers)
    }
    fn flush(&mut self) -> io::Result<()> {
        self.writable_native()?.flush()
    }
}
impl Write for &File {
    fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
        self.writable_native()?.write(buffer)
    }
    fn write_vectored(&mut self, buffers: &[io::IoSlice<'_>]) -> io::Result<usize> {
        self.writable_native()?.write_vectored(buffers)
    }
    fn flush(&mut self) -> io::Result<()> {
        self.writable_native()?.flush()
    }
}
impl Seek for File {
    fn seek(&mut self, position: SeekFrom) -> io::Result<u64> {
        self.seek_sequential(position)
    }
    fn stream_position(&mut self) -> io::Result<u64> {
        self.seek_sequential(SeekFrom::Current(0))
    }
}
impl Seek for &File {
    fn seek(&mut self, position: SeekFrom) -> io::Result<u64> {
        self.seek_sequential(position)
    }
    fn stream_position(&mut self) -> io::Result<u64> {
        self.seek_sequential(SeekFrom::Current(0))
    }
}

#[derive(Debug, Clone)]
pub struct OpenOptions {
    inner: std::fs::OpenOptions,
    kind: DescriptorKind,
    read: bool,
    mutable: [bool; 5],
    native_options: bool,
}

impl Default for OpenOptions {
    fn default() -> Self {
        Self::new()
    }
}

impl OpenOptions {
    pub fn new() -> Self {
        Self {
            inner: std::fs::OpenOptions::new(),
            kind: DescriptorKind::Transient,
            read: false,
            mutable: [false; 5],
            native_options: false,
        }
    }
    pub fn read(&mut self, value: bool) -> &mut Self {
        self.read = value;
        self.inner.read(value);
        self
    }
    pub fn write(&mut self, value: bool) -> &mut Self {
        self.mutable[0] = value;
        self.inner.write(value);
        self
    }
    pub fn append(&mut self, value: bool) -> &mut Self {
        self.mutable[1] = value;
        self.inner.append(value);
        self
    }
    pub fn truncate(&mut self, value: bool) -> &mut Self {
        self.mutable[2] = value;
        self.inner.truncate(value);
        self
    }
    pub fn create(&mut self, value: bool) -> &mut Self {
        self.mutable[3] = value;
        self.inner.create(value);
        self
    }
    pub fn create_new(&mut self, value: bool) -> &mut Self {
        self.mutable[4] = value;
        self.inner.create_new(value);
        self
    }
    pub fn descriptor_kind(&mut self, kind: DescriptorKind) -> &mut Self {
        self.kind = kind;
        self
    }

    pub fn open(&self, path: impl AsRef<Path>) -> io::Result<File> {
        let path = path.as_ref();
        self.open_with_context(path, &context_for_path(path)?)
    }

    pub(crate) fn open_with_context(
        &self,
        path: &Path,
        context: &FileOpenContext,
    ) -> io::Result<File> {
        // Unbound native files do not need an immutable cache allocation.
        let handles = context.state.existing_immutable_handles();
        let mutable = self.mutable.iter().any(|flag| *flag);
        if self.read
            && !mutable
            && !self.native_options
            && self.kind == DescriptorKind::Transient
            && let Some(handles) = handles.as_ref()
            && let Some(binding) = handles.binding(path)?
        {
            return Ok(File {
                backing: FileBacking::Immutable(Box::new(ImmutableFile {
                    binding,
                    handles: handles.clone(),
                    context: context.clone(),
                    cursor: Arc::new(std::sync::Mutex::new(0)),
                })),
            });
        }
        if (mutable || self.native_options)
            && !self.mutable[4]
            && let Some(handles) = handles.as_ref()
            && handles.binding(path)?.is_some()
        {
            // Native options can override access mode or request truncation.
            // Give opaque native opens a private inode too. create_new must
            // still reject an existing alias without changing its binding.
            handles.detach_for_write(path, context, self.mutable[2])?;
        }
        // COW charges each of its native handles before opening it, then
        // closes the candidate before acquiring this final handle's capacity.
        let permit = context.acquire(self.kind)?;
        // Later readers must see the mutable path. Existing logical readers
        // retain their captured immutable identity and remain snapshot-safe.
        // Invalidate before the native open can truncate or modify the path.
        if (mutable || self.native_options)
            && !self.mutable[4]
            && let Some(handles) = handles.as_ref()
        {
            handles.unbind(path)?;
        }
        let inner = self
            .inner
            .open(path)
            .map_err(|error| context.map_open_error(error))?;
        if self.mutable[4]
            && let Some(handles) = handles
        {
            // A rejected create_new must preserve the existing alias. Clear
            // a stale binding only after native creation actually succeeds.
            handles.unbind(path)?;
        }
        Ok(File {
            backing: FileBacking::Native(NativeFile { inner, permit }),
        })
    }
}

#[cfg(unix)]
impl std::os::unix::fs::OpenOptionsExt for OpenOptions {
    fn mode(&mut self, mode: u32) -> &mut Self {
        self.inner.mode(mode);
        self
    }
    fn custom_flags(&mut self, flags: i32) -> &mut Self {
        self.native_options = true;
        self.inner.custom_flags(flags);
        self
    }
}

#[cfg(windows)]
impl std::os::windows::fs::OpenOptionsExt for OpenOptions {
    fn access_mode(&mut self, mode: u32) -> &mut Self {
        self.native_options = true;
        self.inner.access_mode(mode);
        self
    }
    fn share_mode(&mut self, mode: u32) -> &mut Self {
        self.native_options = true;
        self.inner.share_mode(mode);
        self
    }
    fn custom_flags(&mut self, flags: u32) -> &mut Self {
        self.native_options = true;
        self.inner.custom_flags(flags);
        self
    }
    fn attributes(&mut self, attributes: u32) -> &mut Self {
        self.native_options = true;
        self.inner.attributes(attributes);
        self
    }
    fn security_qos_flags(&mut self, flags: u32) -> &mut Self {
        self.native_options = true;
        self.inner.security_qos_flags(flags);
        self
    }
}

pub fn read(path: impl AsRef<Path>) -> io::Result<Vec<u8>> {
    let mut file = File::open(path)?;
    let mut bytes = Vec::new();
    file.read_to_end(&mut bytes)?;
    Ok(bytes)
}

pub fn read_to_string(path: impl AsRef<Path>) -> io::Result<String> {
    let mut file = File::open(path)?;
    let mut text = String::new();
    file.read_to_string(&mut text)?;
    Ok(text)
}

pub fn write(path: impl AsRef<Path>, contents: impl AsRef<[u8]>) -> io::Result<()> {
    File::create(path)?.write_all(contents.as_ref())
}

pub fn copy(source: impl AsRef<Path>, destination: impl AsRef<Path>) -> io::Result<u64> {
    let mut source = File::open(source)?;
    let metadata = source.metadata()?;
    if !metadata.is_file() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "copy source is not a regular file",
        ));
    }
    let mut destination = File::create(destination)?;
    let copied = io::copy(&mut source, &mut destination)?;
    destination.set_permissions(metadata.permissions())?;
    Ok(copied)
}

fn temporary<T>(path: &Path, operation: impl FnOnce() -> io::Result<T>) -> io::Result<T> {
    let context = context_for_path(path)?;
    let _permit = context.acquire(DescriptorKind::Transient)?;
    operation().map_err(|error| context.map_open_error(error))
}

fn two_paths<T>(
    source: &Path,
    destination: &Path,
    operation: impl FnOnce() -> io::Result<T>,
) -> io::Result<T> {
    let source_context = context_for_path(source)?;
    let destination_context = context_for_path(destination)?;
    let _source_permit = source_context.acquire(DescriptorKind::Transient)?;
    if Arc::ptr_eq(&source_context.state, &destination_context.state) {
        // One filesystem operation in one project is one temporary admission.
        operation().map_err(|error| source_context.map_open_error(error))
    } else {
        let _destination_permit = destination_context.acquire(DescriptorKind::Transient)?;
        operation().map_err(|error| destination_context.map_open_error(error))
    }
}

pub fn metadata(path: impl AsRef<Path>) -> io::Result<Metadata> {
    temporary(path.as_ref(), || std::fs::metadata(path.as_ref()))
}
pub fn try_exists(path: impl AsRef<Path>) -> io::Result<bool> {
    match metadata(path) {
        Ok(_) => Ok(true),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(error),
    }
}
pub fn symlink_metadata(path: impl AsRef<Path>) -> io::Result<Metadata> {
    temporary(path.as_ref(), || std::fs::symlink_metadata(path.as_ref()))
}
pub fn canonicalize(path: impl AsRef<Path>) -> io::Result<PathBuf> {
    temporary(path.as_ref(), || std::fs::canonicalize(path.as_ref()))
}
pub fn create_dir(path: impl AsRef<Path>) -> io::Result<()> {
    temporary(path.as_ref(), || std::fs::create_dir(path.as_ref()))
}
pub fn create_dir_all(path: impl AsRef<Path>) -> io::Result<()> {
    temporary(path.as_ref(), || std::fs::create_dir_all(path.as_ref()))
}
pub fn remove_file(path: impl AsRef<Path>) -> io::Result<()> {
    let path = path.as_ref();
    temporary(path, || std::fs::remove_file(path))?;
    unbind_immutable_path(path)
}

pub(crate) fn unbind_immutable_path(path: &Path) -> io::Result<()> {
    if let Some(handles) = context_for_path(path)?.state.existing_immutable_handles() {
        handles.unbind(path)?;
    }
    Ok(())
}
pub fn remove_dir(path: impl AsRef<Path>) -> io::Result<()> {
    temporary(path.as_ref(), || std::fs::remove_dir(path.as_ref()))
}
pub fn rename(source: impl AsRef<Path>, destination: impl AsRef<Path>) -> io::Result<()> {
    two_paths(source.as_ref(), destination.as_ref(), || {
        std::fs::rename(source.as_ref(), destination.as_ref())
    })?;
    // Captured logical readers retain their immutable identity, while future
    // opens must observe the replacement or the removed source name.
    for path in [source.as_ref(), destination.as_ref()] {
        unbind_immutable_path(path)?;
    }
    Ok(())
}
pub fn hard_link(source: impl AsRef<Path>, destination: impl AsRef<Path>) -> io::Result<()> {
    two_paths(source.as_ref(), destination.as_ref(), || {
        std::fs::hard_link(source.as_ref(), destination.as_ref())
    })
}

#[derive(Debug)]
pub struct ReadDir {
    inner: std::fs::ReadDir,
    permit: Arc<DescriptorPermit>,
}

#[derive(Debug)]
pub struct DirEntry {
    inner: std::fs::DirEntry,
    // Unix entries can retain the directory FD after ReadDir itself is dropped.
    permit: Arc<DescriptorPermit>,
}

pub fn read_dir(path: impl AsRef<Path>) -> io::Result<ReadDir> {
    let path = path.as_ref();
    let context = context_for_path(path)?;
    read_dir_with_context(path, &context)
}

pub(crate) fn read_dir_with_context(path: &Path, context: &FileOpenContext) -> io::Result<ReadDir> {
    let permit = Arc::new(context.acquire(DescriptorKind::Transient)?);
    let inner = std::fs::read_dir(path).map_err(|error| context.map_open_error(error))?;
    Ok(ReadDir { inner, permit })
}

impl Iterator for ReadDir {
    type Item = io::Result<DirEntry>;
    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next().map(|entry| {
            entry.map(|inner| DirEntry {
                inner,
                permit: self.permit.clone(),
            })
        })
    }
}

impl DirEntry {
    pub fn path(&self) -> PathBuf {
        self.inner.path()
    }
    pub fn file_name(&self) -> std::ffi::OsString {
        self.inner.file_name()
    }
    pub fn file_type(&self) -> io::Result<FileType> {
        self.inner.file_type()
    }
    pub fn metadata(&self) -> io::Result<Metadata> {
        let context = &self.permit.context;
        let _permit = context.acquire(DescriptorKind::Transient)?;
        self.inner
            .metadata()
            .map_err(|error| context.map_open_error(error))
    }
}

/// Close each directory batch before descending: depth and breadth never retain
/// one native directory descriptor per node in the deletion tree.
pub fn remove_dir_all(path: impl AsRef<Path>) -> io::Result<()> {
    let path = path.as_ref();
    let root = symlink_metadata(path)?;
    if root.file_type().is_symlink() {
        #[cfg(windows)]
        if root.is_dir() {
            return remove_dir(path);
        }
        return remove_file(path);
    }
    if !root.is_dir() {
        return Err(io::Error::new(
            io::ErrorKind::NotADirectory,
            "removal root is not a directory",
        ));
    }
    let mut pending = vec![path.to_path_buf()];
    while let Some(directory) = pending.pop() {
        let children = {
            let entries = read_dir(&directory)?;
            entries
                .take(64)
                .map(|entry| {
                    let entry = entry?;
                    Ok((entry.path(), entry.file_type()?))
                })
                .collect::<io::Result<Vec<_>>>()?
        };
        if children.is_empty() {
            remove_dir(&directory)?;
        } else {
            pending.push(directory);
            for (child, kind) in children {
                if kind.is_dir() && !kind.is_symlink() {
                    pending.push(child);
                } else {
                    #[cfg(windows)]
                    if kind.is_dir() && kind.is_symlink() {
                        remove_dir(child)?;
                        continue;
                    }
                    remove_file(child)?;
                }
            }
        }
    }
    Ok(())
}
