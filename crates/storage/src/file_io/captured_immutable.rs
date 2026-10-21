// Copyright 2026 Nowledge
// Licensed under the Apache License, Version 2.0.

//! Captured reopen authority for immutable managed content. Keys own memory,
//! never source custody; idle cache entries therefore cannot keep roots alive.

use super::*;
use crate::immutable_object::ObjectReference;
use hawdb_core::{RuntimeMemoryPermit, SharedAllocation as Shared};
use std::cmp::Ordering;
use std::mem::size_of;
use std::ptr::NonNull;

/// Erase the concrete lease without releasing it before freeing its own box.
/// No borrowed payload, clone or raw pointer escapes this owner.
struct OwnedMemory {
    pointer: NonNull<()>,
    release: unsafe fn(NonNull<()>),
    bytes: u64,
}
impl std::fmt::Debug for OwnedMemory {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OwnedMemory")
            .field("bytes", &self.bytes)
            .finish()
    }
}
impl OwnedMemory {
    fn new<M: RuntimeMemoryPermit + 'static>(memory: M) -> Self {
        unsafe fn release<M: RuntimeMemoryPermit>(pointer: NonNull<()>) {
            // SAFETY: new allocated exactly this M, and this owner releases it
            // once. Moving M out frees its box before the capacity owner dies.
            let memory = *unsafe { Box::from_raw(pointer.cast::<M>().as_ptr()) };
            drop(memory);
        }
        let bytes = memory.bytes();
        let pointer = NonNull::from(Box::leak(Box::new(memory))).cast();
        Self {
            pointer,
            release: release::<M>,
            bytes,
        }
    }
}
// SAFETY: new accepts only Send + Sync RuntimeMemoryPermit payloads. The sole
// operation is destruction; pointers/references to the erased payload escape
// neither the constructor nor the owner, and there is no Clone implementation.
unsafe impl Send for OwnedMemory {}
// SAFETY: shared access exposes only the copied byte count through Debug.
unsafe impl Sync for OwnedMemory {}
impl Drop for OwnedMemory {
    fn drop(&mut self) {
        // SAFETY: this unique owner invokes its matching release exactly once.
        unsafe { (self.release)(self.pointer) };
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum CapturedIdentity {
    Managed(ObjectReference),
    Native {
        identity: crate::native_file_identity::NativeIdentity,
        length: u64,
    },
}

#[derive(Debug)]
pub(crate) struct SourceKey {
    pub(crate) capture: u64,
    pub(crate) name: Box<str>,
    pub(crate) identity: CapturedIdentity,
    _memory: OwnedMemory,
}
impl SourceKey {
    fn identity(&self) -> (u64, &str, CapturedIdentity) {
        (self.capture, &self.name, self.identity)
    }
}
impl PartialEq for SourceKey {
    fn eq(&self, other: &Self) -> bool {
        self.identity() == other.identity()
    }
}
impl Eq for SourceKey {}
impl PartialOrd for SourceKey {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for SourceKey {
    fn cmp(&self, other: &Self) -> Ordering {
        self.identity().cmp(&other.identity())
    }
}

#[derive(Debug, Clone)]
pub(crate) enum CacheKey {
    Managed(ObjectReference),
    Captured(Shared<SourceKey>),
}
impl CacheKey {
    pub(crate) fn reference(&self) -> Option<ObjectReference> {
        match self {
            Self::Managed(reference) => Some(*reference),
            Self::Captured(key) => match key.identity {
                CapturedIdentity::Managed(reference) => Some(reference),
                CapturedIdentity::Native { .. } => None,
            },
        }
    }
    pub(crate) fn capture(&self) -> Option<u64> {
        match self {
            Self::Managed(_) => None,
            Self::Captured(key) => Some(key.capture),
        }
    }
}
impl PartialEq for CacheKey {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}
impl Eq for CacheKey {}
impl PartialOrd for CacheKey {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for CacheKey {
    fn cmp(&self, other: &Self) -> Ordering {
        match (self, other) {
            (Self::Managed(a), Self::Managed(b)) => a.cmp(b),
            (Self::Captured(a), Self::Captured(b)) => (**a).cmp(b),
            (Self::Managed(_), Self::Captured(_)) => Ordering::Less,
            (Self::Captured(_), Self::Managed(_)) => Ordering::Greater,
        }
    }
}

#[derive(Debug)]
pub(crate) struct CapturedSource {
    pub(crate) directory: Arc<SourceDirectory>,
    pub(crate) key: Shared<SourceKey>,
    // May retain the caller's process-lock/pin custody, but no execution slot.
    // This owner is never copied into the project cache's keys or alias map.
    _custody: OwnedMemory,
}

impl File {
    /// Capacity retained by the source-scoped key and cache node. The receipt
    /// must own memory only: putting root custody here creates a cache cycle.
    #[doc(hidden)]
    pub fn captured_immutable_key_bytes<M: RuntimeMemoryPermit + 'static>(
        name: &str,
    ) -> io::Result<usize> {
        source_directory::validate_child_name(name)?;
        (size_of::<SourceKey>() + size_of::<M>() + 2 * size_of::<usize>() + 4096)
            .checked_add(name.len())
            .ok_or_else(|| io::Error::other("captured key capacity overflow"))
    }

    /// Capacity for shared logical-file, cursor and source-binding metadata.
    /// The caller also admits its concrete custody owner's allocation.
    #[doc(hidden)]
    pub fn captured_immutable_binding_bytes<M: RuntimeMemoryPermit + 'static>() -> usize {
        size_of::<ImmutableFile>()
            + size_of::<M>()
            + size_of::<CapturedSource>()
            + size_of::<std::sync::Mutex<u64>>()
            + 6 * size_of::<usize>()
    }

    /// A lazy immutable reference whose every cold reopen uses the captured
    /// directory. `reference` must come from the caller's validated source
    /// identity, not an unbound pathname's mutable alias-map entry.
    ///
    /// Cold reads preserve complete managed-object identity validation and
    /// project-wide eviction/coalescing. Construction performs no payload I/O
    /// or native open. Receipts precede allocation and survive physical shared
    /// destruction; custody must retain process authority and exact pins where
    /// required. This primitive does not establish logical root incarnation,
    /// immutable-name publication, GC or a public serving capability.
    #[doc(hidden)]
    pub fn open_captured_immutable<
        K: RuntimeMemoryPermit + 'static,
        C: RuntimeMemoryPermit + 'static,
    >(
        directory: Arc<SourceDirectory>,
        context: &FileOpenContext,
        name: &str,
        reference: ObjectReference,
        key_memory: K,
        custody_memory: C,
    ) -> io::Result<Self> {
        Self::captured_file(
            directory,
            context,
            name,
            CapturedIdentity::Managed(reference),
            key_memory,
            custody_memory,
        )
    }

    /// Capture a native immutable child under the caller's publication/pin
    /// authority. Metadata acquisition is admitted and directory-relative; no
    /// payload is read. Every cold reopen checks the held-file identity and
    /// length. Callers must enforce no-overwrite and retain exact artifact pins;
    /// physical file IDs alone do not prove persisted logical incarnation.
    /// Format/block integrity remains the decoder's responsibility. Managed
    /// artifacts must use `open_captured_immutable` with their trusted digest.
    #[doc(hidden)]
    pub fn open_captured_native_immutable<
        K: RuntimeMemoryPermit + 'static,
        C: RuntimeMemoryPermit + 'static,
    >(
        directory: Arc<SourceDirectory>,
        context: &FileOpenContext,
        name: &str,
        key_memory: K,
        custody_memory: C,
        control: &mut impl ReadAdmissionControl,
    ) -> io::Result<Self> {
        Self::validate_captured_capacity(&directory, context, name, &key_memory, &custody_memory)?;
        control.checkpoint()?;
        let file =
            directory.open_child_kind(&context.for_operation(), name, DescriptorKind::Transient)?;
        let metadata = file.metadata()?;
        let identity = file.with_native(crate::native_file_identity::of_file)?;
        control.checkpoint()?;
        let identity = CapturedIdentity::Native {
            identity,
            length: metadata.len(),
        };
        // Close the acquisition descriptor before returning a lazy binding.
        drop(file);
        Self::captured_file(
            directory,
            context,
            name,
            identity,
            key_memory,
            custody_memory,
        )
    }

    fn validate_captured_capacity<
        K: RuntimeMemoryPermit + 'static,
        C: RuntimeMemoryPermit + 'static,
    >(
        directory: &SourceDirectory,
        context: &FileOpenContext,
        name: &str,
        key_memory: &K,
        custody_memory: &C,
    ) -> io::Result<()> {
        let key_bytes = Self::captured_immutable_key_bytes::<K>(name)?;
        if !directory.shares_project_context(context) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "foreign captured file context",
            ));
        }
        if key_memory.bytes() < key_bytes as u64
            || custody_memory.bytes() < Self::captured_immutable_binding_bytes::<C>() as u64
        {
            return Err(io::Error::other("captured file metadata is not admitted"));
        }
        if context.state.existing_immutable_handles().is_none() {
            return Err(io::Error::other(
                "captured immutable file requires an existing project cache",
            ));
        }
        Ok(())
    }

    fn captured_file<K: RuntimeMemoryPermit + 'static, C: RuntimeMemoryPermit + 'static>(
        directory: Arc<SourceDirectory>,
        context: &FileOpenContext,
        name: &str,
        identity: CapturedIdentity,
        key_memory: K,
        custody_memory: C,
    ) -> io::Result<Self> {
        Self::validate_captured_capacity(&directory, context, name, &key_memory, &custody_memory)?;
        let handles = context.state.existing_immutable_handles().ok_or_else(|| {
            io::Error::other("captured immutable file requires an existing project cache")
        })?;
        let binding = match identity {
            CapturedIdentity::Managed(reference) => {
                Some(crate::immutable_files::ImmutableFileBinding {
                    reference,
                    object_path: PathBuf::new(),
                })
            }
            CapturedIdentity::Native { .. } => None,
        };
        let key = Shared::new(SourceKey {
            capture: directory.capture_id(),
            name: name.into(),
            identity,
            _memory: OwnedMemory::new(key_memory),
        });
        directory.mark_cached_child();
        let source = Shared::new(CapturedSource {
            directory,
            key,
            _custody: OwnedMemory::new(custody_memory),
        });
        let file = Shared::new(ImmutableFile {
            binding,
            handles,
            context: context.without_inventory(),
            pinned: None,
            cursor: Arc::new(std::sync::Mutex::new(0)),
            source: Some(source),
        });
        Ok(Self {
            backing: FileBacking::Captured { file, pinned: None },
        })
    }
}

impl ImmutableFile {
    pub(super) fn byte_length(&self) -> u64 {
        match &self.source {
            Some(source) => match source.key.identity {
                CapturedIdentity::Managed(reference) => reference.byte_length,
                CapturedIdentity::Native { length, .. } => length,
            },
            None => {
                self.binding
                    .as_ref()
                    .expect("legacy immutable file owns managed binding")
                    .reference
                    .byte_length
            }
        }
    }
    pub(super) fn get(&self) -> io::Result<Arc<File>> {
        self.get_admitted(|_| Ok(())).map(|(file, _)| file)
    }
    pub(super) fn get_admitted(
        &self,
        admission: impl FnMut(u64) -> io::Result<()>,
    ) -> io::Result<(Arc<File>, u64)> {
        struct Control<F>(F);
        impl<F: FnMut(u64) -> io::Result<()>> ReadAdmissionControl for Control<F> {
            fn checkpoint(&mut self) -> io::Result<()> {
                Ok(())
            }
            fn admit_validation(&mut self, bytes: u64) -> io::Result<()> {
                (self.0)(bytes)
            }
        }
        self.get_controlled(&mut Control(admission))
    }
    pub(super) fn get_controlled(
        &self,
        control: &mut impl ReadAdmissionControl,
    ) -> io::Result<(Arc<File>, u64)> {
        match &self.source {
            Some(source) => self.handles.get_captured_controlled(
                self.binding.as_ref(),
                source,
                &self.context.for_operation(),
                control,
            ),
            None => self.handles.get_controlled(
                self.binding
                    .as_ref()
                    .ok_or_else(|| io::Error::other("missing managed binding"))?,
                &self.context,
                control,
            ),
        }
    }
}

#[cfg(all(test, any(unix, windows)))]
mod tests;
