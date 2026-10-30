// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! Captured native directory authority; caller owns publication exclusion and
//! artifact registrations separately. No pathname is retained or consulted by
//! child operations. This primitive does not qualify GC, durability or memory.

use super::{File, FileBacking, NativeFile};
use crate::file_descriptors::{DescriptorKind, DescriptorPermit, FileOpenContext};
use std::ffi::OsStr;
use std::io;
use std::path::Path;

mod control_candidate;
pub use control_candidate::{CapturedControlCandidate, PendingControlPublication};
mod directory_candidate;
pub use directory_candidate::CapturedDirectoryCandidate;
mod directory_component;
pub use directory_component::CapturedDirectoryComponentCandidate;

#[cfg(unix)]
#[path = "source_directory/unix.rs"]
mod platform;
#[cfg(windows)]
#[path = "source_directory/windows.rs"]
mod platform;
#[cfg(not(any(unix, windows)))]
mod platform {
    use super::*;
    pub(super) fn create_directory_component(
        _root: &std::fs::File,
        _name: DirectoryComponent<'_>,
    ) -> io::Result<()> {
        Err(unsupported())
    }
    pub(super) fn open_directory_component(
        _root: &std::fs::File,
        _name: DirectoryComponent<'_>,
    ) -> io::Result<std::fs::File> {
        Err(unsupported())
    }
    pub(super) fn create_directory(_root: &std::fs::File, _name: ChildName<'_>) -> io::Result<()> {
        Err(unsupported())
    }
    pub(super) fn link_child_new(
        _source: &std::fs::File,
        _source_name: ChildName<'_>,
        _expected: &std::fs::File,
        _target: &std::fs::File,
        _target_name: ChildName<'_>,
        _context: &FileOpenContext,
    ) -> io::Result<()> {
        Err(unsupported())
    }
    pub(super) fn create_control_candidate(
        _root: &std::fs::File,
        _name: ChildName<'_>,
    ) -> io::Result<std::fs::File> {
        Err(unsupported())
    }
    pub(super) fn matches_child_file(
        _root: &std::fs::File,
        _name: ChildName<'_>,
        _file: &std::fs::File,
        _context: &FileOpenContext,
    ) -> io::Result<bool> {
        Err(unsupported())
    }
    pub(super) fn open_wal_append_child(
        _root: &std::fs::File,
        _name: ChildName<'_>,
    ) -> io::Result<std::fs::File> {
        Err(unsupported())
    }
    pub(super) fn open_mutable_child(
        _root: &std::fs::File,
        _name: ChildName<'_>,
    ) -> io::Result<std::fs::File> {
        Err(unsupported())
    }
    pub(super) fn replace_control(
        _root: &std::fs::File,
        _file: &std::fs::File,
        _source: ChildName<'_>,
        _target: ChildName<'_>,
    ) -> io::Result<()> {
        Err(unsupported())
    }
    pub(super) fn open_root(_path: &Path) -> io::Result<std::fs::File> {
        Err(unsupported())
    }
    pub(super) fn open_child(
        _root: &std::fs::File,
        _name: ChildName<'_>,
    ) -> io::Result<std::fs::File> {
        Err(unsupported())
    }
    pub(super) fn open_directory(
        _root: &std::fs::File,
        _name: ChildName<'_>,
    ) -> io::Result<std::fs::File> {
        Err(unsupported())
    }
    pub(super) fn remove_child(
        _root: &std::fs::File,
        _name: ChildName<'_>,
        _context: &FileOpenContext,
    ) -> io::Result<()> {
        Err(unsupported())
    }
    pub(super) fn remove_directory(
        _root: &std::fs::File,
        _name: ChildName<'_>,
        _context: &FileOpenContext,
    ) -> io::Result<()> {
        Err(unsupported())
    }
    pub(super) fn open_lock_file(
        _root: &std::fs::File,
        _name: ChildName<'_>,
        _mode: SourceDirectoryLockMode,
    ) -> io::Result<std::fs::File> {
        Err(unsupported())
    }
    #[cfg(any(test, feature = "test-support"))]
    pub(super) fn open_recorded_lock(
        _root: &std::fs::File,
        _name: ChildName<'_>,
        _create_new: bool,
    ) -> io::Result<std::fs::File> {
        Err(unsupported())
    }
    pub(super) fn visit_children(
        _root: &std::fs::File,
        _visitor: &mut dyn FnMut(&str) -> io::Result<()>,
    ) -> io::Result<()> {
        Err(unsupported())
    }
    pub(super) fn sync_directory(
        _root: &std::fs::File,
        _context: &FileOpenContext,
    ) -> io::Result<()> {
        Err(unsupported())
    }
    fn unsupported() -> io::Error {
        io::Error::new(
            io::ErrorKind::Unsupported,
            "native source directory requires Unix or Windows",
        )
    }
}

#[cfg(any(test, feature = "test-support"))]
mod power_loss_root;
#[cfg(any(test, feature = "test-support"))]
pub(crate) use power_loss_root::PowerLossRoot;

/// Physical identity copied while a counted directory handle is live. A
/// registry must retain that handle while using the key; this is not a logical
/// incarnation token and must not be reused after all capturing handles close.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct SourceDirectoryIdentity(crate::native_file_identity::NativeIdentity);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SourceDirectoryLockMode {
    ReadExisting,
    ReadWriteCreate,
}

/// Captured native directory authority. Artifact names retain portable ASCII
/// validation; configured directory components use separate native validation.
/// Native handles and their descriptor receipts stay together.
///
/// Capture and every method require the caller's I/O admission. This type does
/// not take publication ownership, register reader pins, exclude concurrent
/// deletion, or authorize a complete source watermark. Recorder contexts bind
/// the held directory and returned files to logical inodes on Linux, Apple and
/// Windows; unsupported platforms and unbound recorder handles refuse.
#[derive(Debug)]
pub struct SourceDirectory {
    root: File,
    context: FileOpenContext,
    identity: SourceDirectoryIdentity,
    capture_id: u64,
    cached_children: std::sync::atomic::AtomicBool,
    #[cfg(any(test, feature = "test-support"))]
    trace: Option<Box<crate::power_loss::NativeDirectoryTrace>>,
}

impl SourceDirectory {
    /// Use the same registered (or standalone) descriptor domain as File::open.
    /// This does not create a new project or a new descriptor allowance.
    pub fn capture_for_path(path: &Path) -> io::Result<Self> {
        Self::capture(path, &crate::file_descriptors::context_for_path(path)?)
    }

    /// Borrow fresh operation capacity from the captured descriptor domain.
    pub fn operation_context(&self) -> FileOpenContext {
        self.context.for_operation()
    }

    /// Use an existing project context; no new registry/domain is created.
    pub fn capture(path: &Path, context: &FileOpenContext) -> io::Result<Self> {
        let native = native_scope(context)?;
        let permit = context.acquire(DescriptorKind::Transient)?;
        #[cfg(any(test, feature = "test-support"))]
        let (inner, trace) = match native._guard.upgrade() {
            Some(core) => {
                let (inner, trace) =
                    crate::power_loss::NativeDirectoryTrace::capture(core, path, context)?;
                (inner, Some(Box::new(trace)))
            }
            None => (
                platform::open_root(path).map_err(|error| context.map_open_error(error))?,
                None,
            ),
        };
        #[cfg(not(any(test, feature = "test-support")))]
        let inner = platform::open_root(path).map_err(|error| context.map_open_error(error))?;
        #[cfg(not(any(test, feature = "test-support")))]
        let _ = native;
        if !inner.metadata()?.is_dir() {
            return Err(io::Error::new(
                io::ErrorKind::NotADirectory,
                "source root is not a directory",
            ));
        }
        let identity = SourceDirectoryIdentity(crate::native_file_identity::of_file(&inner)?);
        Ok(Self {
            identity,
            capture_id: next_capture_id()?,
            cached_children: std::sync::atomic::AtomicBool::new(false),
            root: counted(inner, permit),
            // Keep the project domain, not a producer's reservation inventory.
            // Future operations supply their own fresh reserved capacity.
            context: context.without_inventory(),
            #[cfg(any(test, feature = "test-support"))]
            trace,
        })
    }

    /// No I/O, allocation or native handle acquisition is performed. Keep the
    /// captured directory alive while looking up its physical registry key.
    pub fn identity(&self) -> SourceDirectoryIdentity {
        self.identity
    }

    pub fn same_directory(&self, other: &Self) -> bool {
        self.identity.0 == other.identity.0
    }

    /// Duplicate this held directory with fresh capacity from the same project
    /// domain. This never reopens its pathname or duplicates a process lock.
    /// The caller owns metadata admission and any logical source registrations.
    pub fn duplicate(&self, context: &FileOpenContext) -> io::Result<Self> {
        self.require_context(context)?;
        let scope = native_scope(context)?;
        self.require_scope(&scope)?;
        let permit = context.acquire(DescriptorKind::Transient)?;
        let inner = self
            .root
            .with_native(std::fs::File::try_clone)
            .map_err(|error| context.map_open_error(error))?;
        Ok(Self {
            root: counted(inner, permit),
            context: context.without_inventory(),
            identity: self.identity,
            capture_id: next_capture_id()?,
            cached_children: std::sync::atomic::AtomicBool::new(false),
            #[cfg(any(test, feature = "test-support"))]
            trace: self.trace.clone(),
        })
    }

    /// Conservative owned metadata for a duplicate or generated child capture.
    /// This excludes shared project/model state and performs no I/O/allocation.
    pub fn metadata_bound(&self, child_bytes: usize) -> io::Result<usize> {
        let _ = child_bytes;
        let bytes = std::mem::size_of::<Self>();
        #[cfg(any(test, feature = "test-support"))]
        if let Some(trace) = &self.trace {
            return bytes
                .checked_add(trace.metadata_bound(child_bytes)?)
                .ok_or_else(|| io::Error::other("directory metadata bound overflow"));
        }
        Ok(bytes)
    }

    /// Exclusively create one regular read/write child. A private-directory
    /// owner must retain cleanup evidence before calling this primitive.
    pub fn create_child_new(&self, context: &FileOpenContext, name: &str) -> io::Result<File> {
        self.open_mutable_child(context, name, true)
    }

    /// Pure domain comparison; does not open a file or consume FD inventory.
    pub fn shares_project_context(&self, context: &FileOpenContext) -> bool {
        std::sync::Arc::ptr_eq(&self.context.state, &context.state)
    }

    /// Check an initial/legacy namespace label against this held directory.
    /// This is a binding fence, not logical incarnation or an ABA proof; child
    /// operations must still use the captured directory after the fence.
    #[doc(hidden)]
    pub fn matches_namespace(&self, path: &Path) -> io::Result<bool> {
        #[cfg(windows)]
        {
            // A directory requires BACKUP_SEMANTICS; File::open is for regular
            // files. The fresh capture uses the same counted descriptor domain.
            Ok(self.same_directory(&Self::capture(path, &self.operation_context())?))
        }
        #[cfg(unix)]
        {
            // Capture already pinned the identity with the original handle.
            // Only the namespace can change; retain fresh, counted path lookup.
            Ok(self.identity.0
                == crate::native_file_identity::of_metadata(
                    &self.operation_context().metadata(path)?,
                ))
        }
        #[cfg(not(any(unix, windows)))]
        self.root.matches_native_path(path)
    }

    /// Opens only a hidden `.lock` control file, never truncating an existing
    /// file. Lock acquisition/release and process-registry lifetime belong to
    /// the caller. ReadExisting supports a read-only root without creating data.
    pub fn open_lock_file(
        &self,
        context: &FileOpenContext,
        name: &str,
        mode: SourceDirectoryLockMode,
    ) -> io::Result<File> {
        self.require_context(context)?;
        let name = ChildName::new(name)?;
        if !name.0.starts_with('.') || !name.0.ends_with(".lock") || name.0.len() < 7 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "invalid source ownership lock name",
            ));
        }
        self.open_named_lock(context, name, mode)
    }

    pub(crate) fn open_database_ownership_lock(
        &self,
        context: &FileOpenContext,
        mode: SourceDirectoryLockMode,
    ) -> io::Result<File> {
        self.open_named_lock(
            context,
            ChildName::new(crate::ownership::DATABASE_DIRECTORY_LOCK_FILE)?,
            mode,
        )
    }

    pub(crate) fn open_catalog_metadata_lock(&self, context: &FileOpenContext) -> io::Result<File> {
        self.open_named_lock(
            context,
            ChildName::new(crate::branch_catalog::METADATA_LOCK_FILE)?,
            SourceDirectoryLockMode::ReadWriteCreate,
        )
    }

    fn open_named_lock(
        &self,
        context: &FileOpenContext,
        name: ChildName<'_>,
        mode: SourceDirectoryLockMode,
    ) -> io::Result<File> {
        self.require_context(context)?;
        let _native = native_scope(context)?;
        self.require_scope(&_native)?;
        let permit = context.acquire(DescriptorKind::OwnershipLock)?;
        let open = || {
            self.root
                .with_native(|root| platform::open_lock_file(root, name, mode))
                .map_err(|error| context.map_open_error(error))
        };
        #[cfg(any(test, feature = "test-support"))]
        let (inner, trace) = match self.trace.as_deref() {
            Some(trace) => {
                let (inner, trace) = match mode {
                    SourceDirectoryLockMode::ReadExisting => {
                        trace.open_child(Path::new(name.0), open)?
                    }
                    SourceDirectoryLockMode::ReadWriteCreate => {
                        trace.open_lock(Path::new(name.0), |create_new| {
                            self.root
                                .with_native(|root| {
                                    platform::open_recorded_lock(root, name, create_new)
                                })
                                .map_err(|error| context.map_open_error(error))
                        })?
                    }
                };
                (inner, Some(Box::new(trace)))
            }
            None => (open()?, None),
        };
        #[cfg(not(any(test, feature = "test-support")))]
        let inner = open()?;
        if !inner.metadata()?.is_file() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "source lock is not a regular file",
            ));
        }
        #[cfg(any(test, feature = "test-support"))]
        {
            Ok(counted_traced(inner, permit, trace))
        }
        #[cfg(not(any(test, feature = "test-support")))]
        {
            Ok(counted(inner, permit))
        }
    }

    /// Opens one regular file without following a child symlink/reparse point.
    /// The returned file owns an admitted native handle; it is not an evictable
    /// path-based cache reference. The caller controls its retention lifetime.
    /// A fresh operation context supplies its reserved FD capacity; the
    /// directory capture context is never reused for child admission.
    pub fn open_child(&self, context: &FileOpenContext, name: &str) -> io::Result<File> {
        self.open_child_kind(context, name, DescriptorKind::Transient)
    }

    pub(crate) fn open_child_kind(
        &self,
        context: &FileOpenContext,
        name: &str,
        kind: DescriptorKind,
    ) -> io::Result<File> {
        self.open_existing_child(context, name, kind, false)
    }

    /// Compare a held file with one regular, non-symlink component through this
    /// original root. This is a binding fence, not publication/adoption authority.
    /// Unix uses fstatat without another open/close of a process lock inode;
    /// Windows charges its temporary validation handle to this original context.
    pub fn matches_child_file(
        &self,
        context: &FileOpenContext,
        name: &str,
        file: &File,
    ) -> io::Result<bool> {
        self.require_context(context)?;
        let name = ChildName::new(name)?;
        let scope = native_scope(context)?;
        self.require_scope(&scope)?;
        self.matches_child_file_scoped(context, name, file)
    }

    fn matches_child_file_scoped(
        &self,
        context: &FileOpenContext,
        name: ChildName<'_>,
        file: &File,
    ) -> io::Result<bool> {
        self.root.with_native(|root| {
            file.with_native(|file| platform::matches_child_file(root, name, file, context))
        })
    }

    /// Reuse one immutable regular file in another captured directory without
    /// copying its bytes or resolving either directory's diagnostic pathname.
    /// Both directories must belong to the same project/recorder; `expected`
    /// pins the original source file. The caller excludes source mutation and
    /// target-name reuse and owns the target's cleanup/publication custody.
    ///
    /// Creation is exclusive. A later identity check or lost response can fail
    /// after the link exists: retain original target custody rather than adopt
    /// or retry the name. This does not synchronize the target directory or
    /// grant integrity, source-watermark, publication or retirement authority.
    pub fn link_child_new(
        &self,
        context: &FileOpenContext,
        source_name: &str,
        expected: &File,
        target: &Self,
        target_name: &str,
    ) -> io::Result<()> {
        self.require_context(context)?;
        target.require_context(context)?;
        let FileBacking::Native(native_expected) = &expected.backing else {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "immutable link requires an already held native source",
            ));
        };
        self.require_context(&native_expected.permit.context)?;
        let source_name = ChildName::new(source_name)?;
        let target_name = ChildName::new(target_name)?;
        let scope = native_scope(context)?;
        self.require_scope(&scope)?;
        target.require_scope(&scope)?;
        if !self.matches_child_file_scoped(context, source_name, expected)? {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "immutable link source binding changed",
            ));
        }
        let native = || {
            self.root
                .with_native(|source| {
                    target.root.with_native(|destination| {
                        // The directory recorder owns model exclusion around
                        // this native call. Re-entering the file trace would
                        // recursively lock that same model. Native custody was
                        // checked above; no cache lookup or fresh open occurs.
                        platform::link_child_new(
                            source,
                            source_name,
                            &native_expected.inner,
                            destination,
                            target_name,
                            context,
                        )
                    })
                })
                .map_err(|error| context.map_open_error(error))
        };
        #[cfg(any(test, feature = "test-support"))]
        match (self.trace.as_deref(), target.trace.as_deref()) {
            (Some(source), Some(target)) => source.link_child_new(
                Path::new(source_name.0),
                target,
                Path::new(target_name.0),
                native,
            )?,
            (None, None) => native()?,
            _ => {
                return Err(io::Error::new(
                    io::ErrorKind::Unsupported,
                    "immutable link has mismatched recorder custody",
                ))
            }
        }
        #[cfg(not(any(test, feature = "test-support")))]
        native()?;
        if !target.matches_child_file_scoped(context, target_name, expected)? {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "immutable linked target binding changed",
            ));
        }
        Ok(())
    }

    /// Existing current-generation WAL only: no create, truncate or publication.
    /// Reading and appending share one native object and its original account.
    pub(crate) fn open_wal_append_child(
        &self,
        context: &FileOpenContext,
        name: &str,
    ) -> io::Result<File> {
        self.open_existing_child(context, name, DescriptorKind::MutableWal, true)
    }

    fn open_existing_child(
        &self,
        context: &FileOpenContext,
        name: &str,
        kind: DescriptorKind,
        append: bool,
    ) -> io::Result<File> {
        self.require_context(context)?;
        let name = ChildName::new(name)?;
        let _native = native_scope(context)?;
        self.require_scope(&_native)?;
        let permit = context.acquire(kind)?;
        let open = || {
            self.root
                .with_native(|root| {
                    if append {
                        platform::open_wal_append_child(root, name)
                    } else {
                        platform::open_child(root, name)
                    }
                })
                .map_err(|error| context.map_open_error(error))
        };
        #[cfg(any(test, feature = "test-support"))]
        let (inner, trace) = match self.trace.as_deref() {
            Some(trace) => {
                let (inner, mut trace) = trace.open_child(Path::new(name.0), open)?;
                if append {
                    trace.mark_append();
                }
                (inner, Some(Box::new(trace)))
            }
            None => (open()?, None),
        };
        #[cfg(not(any(test, feature = "test-support")))]
        let inner = open()?;
        if !inner.metadata()?.is_file() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "source child is not a regular file",
            ));
        }
        #[cfg(any(test, feature = "test-support"))]
        {
            Ok(counted_traced(inner, permit, trace))
        }
        #[cfg(not(any(test, feature = "test-support")))]
        {
            Ok(counted(inner, permit))
        }
    }

    /// Catalog writer primitive, not a publication or immutable-file capability.
    /// The catalog owns writer exclusion, sealing and recovery. Existing opens
    /// never create/truncate; new opens are exclusive. Both are captured-relative.
    pub(crate) fn open_mutable_child(
        &self,
        context: &FileOpenContext,
        name: &str,
        create_new: bool,
    ) -> io::Result<File> {
        self.require_context(context)?;
        let name = ChildName::new(name)?;
        let scope = native_scope(context)?;
        self.require_scope(&scope)?;
        let permit = context.acquire(DescriptorKind::Transient)?;
        let open = || {
            self.root
                .with_native(|root| {
                    if create_new {
                        platform::create_control_candidate(root, name)
                    } else {
                        platform::open_mutable_child(root, name)
                    }
                })
                .map_err(|error| context.map_open_error(error))
        };
        #[cfg(any(test, feature = "test-support"))]
        let (inner, trace) = match self.trace.as_deref() {
            Some(trace) => {
                let (inner, trace) = if create_new {
                    trace.create_file(Path::new(name.0), open)?
                } else {
                    trace.open_child(Path::new(name.0), open)?
                };
                (inner, Some(Box::new(trace)))
            }
            None => (open()?, None),
        };
        #[cfg(not(any(test, feature = "test-support")))]
        let inner = open()?;
        if !create_new && !inner.metadata()?.is_file() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "mutable catalog child is not a regular file",
            ));
        }
        #[cfg(any(test, feature = "test-support"))]
        {
            Ok(counted_traced(inner, permit, trace))
        }
        #[cfg(not(any(test, feature = "test-support")))]
        {
            Ok(counted(inner, permit))
        }
    }

    /// Exclusively create one child directory in this captured parent.
    /// Existing entries are never adopted, followed, replaced or removed.
    /// Admission precedes native creation; no descriptor remains open afterwards.
    ///
    /// This does not open the child or synchronize its parent entry. A later
    /// open, barrier or lost response can fail after creation; callers retain
    /// the actual parent and candidate name as cleanup/recovery evidence.
    pub fn create_directory_new(&self, context: &FileOpenContext, name: &str) -> io::Result<()> {
        self.require_context(context)?;
        let name = ChildName::new(name)?;
        let _native = native_scope(context)?;
        self.require_scope(&_native)?;
        let _permit = context.acquire(DescriptorKind::Transient)?;
        let create = || {
            self.root
                .with_native(|root| platform::create_directory(root, name))
        };
        #[cfg(any(test, feature = "test-support"))]
        let result = match self.trace.as_deref() {
            Some(trace) => trace.create_directory(Path::new(name.0), create),
            None => create(),
        };
        #[cfg(not(any(test, feature = "test-support")))]
        let result = create();
        result.map_err(|error| context.map_open_error(error))
    }

    /// Capture one child directory without resolving the parent pathname.
    /// Both authorities have independently counted handles; the child can
    /// outlive its parent. Symlinks and reparse points are not followed.
    /// The caller supplies I/O admission and namespace exclusion separately.
    pub fn open_directory(&self, context: &FileOpenContext, name: &str) -> io::Result<Self> {
        self.require_context(context)?;
        let name = ChildName::new(name)?;
        self.open_directory_with(context, Path::new(name.0), |root| {
            platform::open_directory(root, name)
        })
    }

    fn open_directory_with(
        &self,
        context: &FileOpenContext,
        name: &Path,
        open_native: impl FnOnce(&std::fs::File) -> io::Result<std::fs::File>,
    ) -> io::Result<Self> {
        self.require_context(context)?;
        let _native = native_scope(context)?;
        self.require_scope(&_native)?;
        let permit = context.acquire(DescriptorKind::Transient)?;
        let open = || {
            self.root
                .with_native(open_native)
                .map_err(|error| context.map_open_error(error))
        };
        #[cfg(any(test, feature = "test-support"))]
        let (inner, trace) = match self.trace.as_deref() {
            Some(trace) => {
                let (inner, trace) = trace.open_directory(name, open)?;
                (inner, Some(Box::new(trace)))
            }
            None => (open()?, None),
        };
        #[cfg(not(any(test, feature = "test-support")))]
        let inner = {
            let _ = name;
            open()?
        };
        Self::finish_directory_capture(
            context,
            inner,
            permit,
            #[cfg(any(test, feature = "test-support"))]
            trace,
        )
    }

    fn finish_directory_capture(
        context: &FileOpenContext,
        inner: std::fs::File,
        permit: DescriptorPermit,
        #[cfg(any(test, feature = "test-support"))] trace: Option<
            Box<crate::power_loss::NativeDirectoryTrace>,
        >,
    ) -> io::Result<Self> {
        if !inner.metadata()?.is_dir() {
            return Err(io::Error::new(
                io::ErrorKind::NotADirectory,
                "source child is not a directory",
            ));
        }
        let identity = SourceDirectoryIdentity(crate::native_file_identity::of_file(&inner)?);
        Ok(Self {
            root: counted(inner, permit),
            context: context.without_inventory(),
            identity,
            capture_id: next_capture_id()?,
            cached_children: std::sync::atomic::AtomicBool::new(false),
            #[cfg(any(test, feature = "test-support"))]
            trace,
        })
    }

    /// Unlinks one entry relative to the captured root. The caller must exclude
    /// registration/publication and establish that no live reader needs it.
    /// Namespace durability and replay are separate obligations. The operation
    /// context supplies any extra native FD capacity needed for deletion.
    pub fn remove_child(&self, context: &FileOpenContext, name: &str) -> io::Result<()> {
        self.require_context(context)?;
        let name = ChildName::new(name)?;
        let _native = native_scope(context)?;
        self.require_scope(&_native)?;
        let native = || {
            self.root
                .with_native(|root| platform::remove_child(root, name, context))
                .map_err(|error| context.map_open_error(error))
        };
        #[cfg(any(test, feature = "test-support"))]
        if let Some(trace) = self.trace.as_deref() {
            return trace.remove_child(Path::new(name.0), native);
        }
        native()
    }

    /// Consumer retirement already holds durable name ownership and excludes
    /// readers/publication. Recheck the held child before removing its empty
    /// directory; never follow a replacement or recursively remove evidence.
    pub(crate) fn remove_reclaimed_directory(
        &self,
        context: &FileOpenContext,
        name: &str,
        expected: &Self,
    ) -> io::Result<()> {
        self.require_context(context)?;
        let observed = self.open_directory(context, name)?;
        if !observed.same_directory(expected) {
            return Err(io::Error::other("reclaimed directory binding changed"));
        }
        drop(observed);
        let name = ChildName::new(name)?;
        let scope = native_scope(context)?;
        self.require_scope(&scope)?;
        let native = || {
            self.root
                .with_native(|root| platform::remove_directory(root, name, context))
                .map_err(|error| context.map_open_error(error))
        };
        #[cfg(any(test, feature = "test-support"))]
        if let Some(trace) = self.trace.as_deref() {
            return trace.remove_directory(Path::new(name.0), native);
        }
        native()
    }

    /// Persist this captured directory's direct entries. Windows admits a
    /// separate write-capable handle to the same native directory; failures
    /// propagate and cannot substitute for a successful namespace barrier.
    pub fn sync(&self, context: &FileOpenContext) -> io::Result<()> {
        self.require_context(context)?;
        let scope = native_scope(context)?;
        self.require_scope(&scope)?;
        let native = || {
            self.root
                .with_native(|root| platform::sync_directory(root, context))
                .map_err(|error| context.map_open_error(error))
        };
        #[cfg(any(test, feature = "test-support"))]
        if let Some(trace) = self.trace.as_deref() {
            return trace.sync(native);
        }
        native()
    }

    /// Visit generated artifact names through a fresh native directory cursor.
    /// The caller supplies I/O admission and namespace exclusion: enumeration
    /// alone is not a snapshot. Exceeding the row budget or encountering an
    /// unsupported component fails, even after earlier callbacks succeeded;
    /// callers must not publish partial results. Callback errors stop scanning.
    /// Each invocation admits one extra descriptor before opening its cursor.
    pub fn visit_children(
        &self,
        context: &FileOpenContext,
        maximum_entries: usize,
        mut visitor: impl FnMut(&str) -> io::Result<()>,
    ) -> io::Result<()> {
        self.require_context(context)?;
        // This live counted root prevents attaching a new recorder (attach
        // requires zero open/reserved descriptors). Release registration
        // exclusion before invoking user callbacks, which may perform I/O.
        let _permit = {
            let _native = native_scope(context)?;
            self.require_scope(&_native)?;
            context.acquire(DescriptorKind::Transient)?
        };
        let mut remaining = maximum_entries;
        self.root
            .with_native(|root| {
                platform::visit_children(root, &mut |name| {
                    ChildName::new(name)?;
                    remaining = remaining.checked_sub(1).ok_or_else(|| {
                        io::Error::new(
                            io::ErrorKind::InvalidData,
                            "source directory entry budget exceeded",
                        )
                    })?;
                    visitor(name)
                })
            })
            .map_err(|error| context.map_open_error(error))
    }

    pub(super) fn capture_id(&self) -> u64 {
        self.capture_id
    }

    pub(super) fn mark_cached_child(&self) {
        self.cached_children
            .store(true, std::sync::atomic::Ordering::Relaxed);
    }

    /// Close idle captured-source cache leases before releasing cooperative
    /// process custody. Does not revoke any live logical file or read.
    #[doc(hidden)]
    pub fn retire_cached_children(&self) {
        if self
            .cached_children
            .load(std::sync::atomic::Ordering::Relaxed)
            && let Some(handles) = self.context.state.existing_immutable_handles()
        {
            handles.retire_source(self.capture_id);
        }
    }

    fn require_context(&self, context: &FileOpenContext) -> io::Result<()> {
        if self.shares_project_context(context) {
            Ok(())
        } else {
            Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "source directory operation requires its project context",
            ))
        }
    }

    fn require_scope(&self, scope: &NativeScope<'_>) -> io::Result<()> {
        #[cfg(any(test, feature = "test-support"))]
        match (scope._guard.upgrade(), self.trace.as_deref()) {
            (None, None) => {}
            (Some(core), Some(trace)) if trace.belongs_to(&core) => {}
            _ => {
                return Err(io::Error::new(
                    io::ErrorKind::Unsupported,
                    "source directory is not bound to this native recorder",
                ))
            }
        }
        #[cfg(not(any(test, feature = "test-support")))]
        let _ = scope;
        Ok(())
    }
}

impl Drop for SourceDirectory {
    fn drop(&mut self) {
        self.retire_cached_children();
    }
}

fn next_capture_id() -> io::Result<u64> {
    static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);
    NEXT.fetch_update(
        std::sync::atomic::Ordering::Relaxed,
        std::sync::atomic::Ordering::Relaxed,
        |value| value.checked_add(1),
    )
    .map_err(|_| io::Error::other("source capture identity exhausted"))
}

pub(super) fn validate_child_name(name: &str) -> io::Result<()> {
    ChildName::new(name).map(|_| ())
}

#[cfg(any(test, feature = "test-support"))]
fn counted_traced(
    inner: std::fs::File,
    permit: DescriptorPermit,
    trace: Option<Box<crate::power_loss::NativeTrace>>,
) -> File {
    File {
        backing: FileBacking::Native(NativeFile {
            inner,
            permit,
            trace,
        }),
    }
}

fn counted(inner: std::fs::File, permit: DescriptorPermit) -> File {
    File {
        backing: FileBacking::Native(NativeFile {
            inner,
            permit,
            #[cfg(any(test, feature = "test-support"))]
            trace: None,
        }),
    }
}

/// Bounded native input storage, not a filesystem filename-equivalence rule.
/// Apple permits existing canonical-equivalent spellings beyond NAME_MAX raw
/// bytes/UTF16 units. Its kernel decides create/lookup eligibility separately.
pub(crate) fn native_component_within_input_bound(name: &OsStr) -> bool {
    #[cfg(windows)]
    {
        use std::os::windows::ffi::OsStrExt;
        name.encode_wide().take(256).count() <= 255
    }
    #[cfg(all(unix, target_vendor = "apple"))]
    {
        name.as_encoded_bytes().len() < libc::PATH_MAX as usize
    }
    #[cfg(not(any(windows, all(unix, target_vendor = "apple"))))]
    {
        name.as_encoded_bytes().len() <= 255
    }
}

#[derive(Clone, Copy)]
struct DirectoryComponent<'a>(&'a OsStr);

impl<'a> DirectoryComponent<'a> {
    fn new(name: &'a OsStr) -> io::Result<Self> {
        if !native_component_within_input_bound(name) {
            return Err(invalid_directory_component());
        }
        let mut components = Path::new(name).components();
        if !matches!(components.next(), Some(std::path::Component::Normal(value)) if value == name)
            || components.next().is_some()
        {
            return Err(invalid_directory_component());
        }
        #[cfg(unix)]
        {
            use std::os::unix::ffi::OsStrExt;
            if name.as_bytes().iter().any(|byte| matches!(byte, 0 | b'/')) {
                return Err(invalid_directory_component());
            }
        }
        #[cfg(windows)]
        {
            use std::os::windows::ffi::OsStrExt;
            if name
                .encode_wide()
                .any(|unit| matches!(unit, 0..=31 | 34 | 42 | 47 | 58 | 60 | 62 | 63 | 92 | 124))
            {
                return Err(invalid_directory_component());
            }
        }
        #[cfg(not(any(unix, windows)))]
        return Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "native directory components require Unix or Windows",
        ));
        #[cfg(any(unix, windows))]
        Ok(Self(name))
    }
}

fn invalid_directory_component() -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidInput,
        "invalid native directory component",
    )
}

#[derive(Clone, Copy)]
struct ChildName<'a>(&'a str);
impl<'a> ChildName<'a> {
    fn new(name: &'a str) -> io::Result<Self> {
        let name = Self(name);
        // Engine artifact names fit one portable component and fixed native
        // scratch. Reject traversal, streams, separators and NUL before I/O.
        if name.0.is_empty()
            || name.0.len() > 255
            || matches!(name.0, "." | "..")
            || !name
                .0
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'))
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "invalid source artifact name",
            ));
        }
        Ok(name)
    }
}

struct NativeScope<'a> {
    #[cfg(any(test, feature = "test-support"))]
    _guard: std::sync::MutexGuard<'a, std::sync::Weak<crate::power_loss::ModelCore>>,
    #[cfg(not(any(test, feature = "test-support")))]
    _borrow: std::marker::PhantomData<&'a FileOpenContext>,
}
fn native_scope(context: &FileOpenContext) -> io::Result<NativeScope<'_>> {
    #[cfg(any(test, feature = "test-support"))]
    {
        // Hold registration exclusion for the operation, so enabling a harness
        // cannot race an unrecorded native namespace mutation.
        let guard = context
            .state
            .power_loss
            .lock()
            .map_err(|_| io::Error::other("power-loss context poisoned"))?;
        Ok(NativeScope { _guard: guard })
    }
    #[cfg(not(any(test, feature = "test-support")))]
    {
        let _ = context;
        Ok(NativeScope {
            _borrow: std::marker::PhantomData,
        })
    }
}

#[cfg(all(test, any(unix, windows)))]
mod tests;

#[cfg(all(test, any(target_os = "linux", target_vendor = "apple", windows)))]
mod recording_tests;
