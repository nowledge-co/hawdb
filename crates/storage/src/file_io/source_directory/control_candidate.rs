// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

use super::*;
use std::io::Write;
use std::sync::Arc;

#[derive(Debug)]
pub(super) enum DirectoryOwner<'a> {
    Borrowed(&'a SourceDirectory),
    Shared(Arc<SourceDirectory>),
}

impl std::ops::Deref for DirectoryOwner<'_> {
    type Target = SourceDirectory;

    fn deref(&self) -> &Self::Target {
        match self {
            Self::Borrowed(directory) => directory,
            Self::Shared(directory) => directory,
        }
    }
}

#[cfg(any(test, feature = "test-support"))]
type RenameHook = std::cell::RefCell<Option<Box<dyn FnOnce()>>>;
#[cfg(any(test, feature = "test-support"))]
type RenameObserver = std::cell::RefCell<Option<Box<dyn FnMut() -> io::Result<()>>>>;
#[cfg(any(test, feature = "test-support"))]
thread_local! {
    static AFTER_RENAME: RenameHook = const { std::cell::RefCell::new(None) };
    static AFTER_EACH_RENAME: RenameObserver = const { std::cell::RefCell::new(None) };
}

#[cfg(any(test, feature = "test-support"))]
impl SourceDirectory {
    /// Observe a bounded operation's control selections for failure injection.
    /// The callback must not recursively publish another control.
    #[doc(hidden)]
    pub fn observe_control_renames<T>(
        mut callback: impl FnMut() + 'static,
        operation: impl FnOnce() -> T,
    ) -> io::Result<T> {
        Self::observe_control_replies(
            move || {
                callback();
                Ok(())
            },
            operation,
        )
    }

    /// Inject an IO/lost-response failure after native selection without
    /// pretending that the completed namespace effect rolled back.
    #[doc(hidden)]
    pub fn observe_control_replies<T>(
        callback: impl FnMut() -> io::Result<()> + 'static,
        operation: impl FnOnce() -> T,
    ) -> io::Result<T> {
        AFTER_EACH_RENAME.with(|hook| {
            let mut slot = hook.borrow_mut();
            if slot.is_some() {
                return Err(io::Error::new(
                    io::ErrorKind::WouldBlock,
                    "a control observer is already installed",
                ));
            }
            *slot = Some(Box::new(callback));
            Ok(())
        })?;
        struct Reset;
        impl Drop for Reset {
            fn drop(&mut self) {
                AFTER_EACH_RENAME.with(|hook| *hook.borrow_mut() = None);
            }
        }
        let reset = Reset;
        let result = operation();
        drop(reset);
        Ok(result)
    }

    /// Test-only, thread-local observation after one successful native rename
    /// and before its completion barriers. The scope resets on return/unwind.
    #[doc(hidden)]
    pub fn observe_next_control_rename<T>(
        callback: impl FnOnce() + 'static,
        operation: impl FnOnce() -> T,
    ) -> io::Result<T> {
        AFTER_RENAME.with(|hook| {
            let mut slot = hook.borrow_mut();
            if slot.is_some() {
                return Err(io::Error::new(
                    io::ErrorKind::WouldBlock,
                    "a control rename observer is already installed",
                ));
            }
            *slot = Some(Box::new(callback));
            Ok(())
        })?;
        struct Reset;
        impl Drop for Reset {
            fn drop(&mut self) {
                AFTER_RENAME.with(|hook| *hook.borrow_mut() = None);
            }
        }
        let reset = Reset;
        let result = operation();
        drop(reset);
        Ok(result)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum State {
    Writable,
    Sealed,
    Attempted,
    Complete,
    Discarded,
}

/// A new mutable-control candidate in one captured directory. The caller owns
/// publication exclusion, dependency durability and recovery/cleanup custody.
/// This is not an immutable-artifact publisher or a Consumer activation token.
/// Do not target ownership locks or managed immutable aliases with this API.
/// Drop closes handles only: failed or uncertain publication never deletes data.
#[doc(hidden)]
#[derive(Debug)]
pub struct CapturedControlCandidate<'a> {
    file: File,
    directory: DirectoryOwner<'a>,
    name: [u8; 256],
    length: usize,
    state: State,
}

/// A selected control whose durability is pending. It exclusively borrows the
/// attempted candidate, retaining its file/directory custody through the fence
/// release. Drop performs no rollback, acknowledgement or namespace cleanup.
#[doc(hidden)]
#[derive(Debug)]
#[must_use = "finish durability or retain recovery custody for the selected control"]
pub struct PendingControlPublication<'candidate, 'directory> {
    candidate: &'candidate mut CapturedControlCandidate<'directory>,
    target: [u8; 256],
    length: usize,
}

impl SourceDirectory {
    /// Exclusively create a writable regular control candidate without using a
    /// pathname. The returned owner borrows this capture, not the operation's
    /// FD inventory. Existing entries, including symlinks, are never adopted.
    #[doc(hidden)]
    pub fn create_control_candidate(
        &self,
        context: &FileOpenContext,
        name: &str,
    ) -> io::Result<CapturedControlCandidate<'_>> {
        self.require_context(context)?;
        let name = ChildName::new(name)?;
        let scope = native_scope(context)?;
        self.require_scope(&scope)?;
        let permit = context.acquire(DescriptorKind::Transient)?;
        let open = || {
            self.root
                .with_native(|root| platform::create_control_candidate(root, name))
                .map_err(|error| context.map_open_error(error))
        };
        #[cfg(any(test, feature = "test-support"))]
        let file = match self.trace.as_deref() {
            Some(trace) => {
                let (inner, trace) = trace.create_file(Path::new(name.0), open)?;
                counted_traced(inner, permit, Some(Box::new(trace)))
            }
            None => counted(open()?, permit),
        };
        #[cfg(not(any(test, feature = "test-support")))]
        let file = counted(open()?, permit);
        let mut bytes = [0; 256];
        bytes[..name.0.len()].copy_from_slice(name.0.as_bytes());
        Ok(CapturedControlCandidate {
            file,
            directory: DirectoryOwner::Borrowed(self),
            name: bytes,
            length: name.0.len(),
            state: State::Writable,
        })
    }
}

impl SourceDirectory {
    /// Retain an existing shared capture without duplicating its native handle
    /// or preserving a producer's FD inventory. This still grants no exclusion.
    #[doc(hidden)]
    pub fn create_owned_control_candidate(
        self: &Arc<Self>,
        context: &FileOpenContext,
        name: &str,
    ) -> io::Result<CapturedControlCandidate<'static>> {
        let CapturedControlCandidate {
            file,
            name,
            length,
            state,
            ..
        } = self.create_control_candidate(context, name)?;
        Ok(CapturedControlCandidate {
            file,
            directory: DirectoryOwner::Shared(self.clone()),
            name,
            length,
            state,
        })
    }
}

impl<'directory> CapturedControlCandidate<'directory> {
    /// Complete and synchronize candidate bytes before attempting publication.
    /// Successful sealing permanently removes this owner's write capability.
    pub fn seal(&mut self) -> io::Result<()> {
        self.require_writable()?;
        self.file.sync_all()?;
        self.state = State::Sealed;
        Ok(())
    }

    /// Replace one mutable head relative to the same capture, then synchronize
    /// the held candidate and captured directory. An error may follow a complete
    /// effect. Every attempt revokes further writes/retries; retain the actual
    /// publication lease and all recovery dependencies until outcome resolution.
    /// The caller keeps candidate names and contents immutable under publication exclusion.
    pub fn replace_head(&mut self, context: &FileOpenContext, name: &str) -> io::Result<()> {
        self.select_head(context, name)?.finish(context)
    }

    /// Perform only the native selection and return owned pending-barrier
    /// custody. A higher-level coordinator must validate/swap its read view
    /// under the same fence, release SourceOwner, then call `finish`.
    pub fn select_head<'candidate>(
        &'candidate mut self,
        context: &FileOpenContext,
        name: &str,
    ) -> io::Result<PendingControlPublication<'candidate, 'directory>> {
        self.directory.require_context(context)?;
        let target = ChildName::new(name)?;
        let source = ChildName(std::str::from_utf8(&self.name[..self.length]).expect("ASCII name"));
        if self.state != State::Sealed || source.0 == target.0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "control candidate must be sealed and have a distinct head name",
            ));
        }
        // Resource refusal and identity checks precede the first namespace
        // attempt. They leave this same sealed owner retryable.
        let _preflight = context.acquire(DescriptorKind::Transient)?;
        {
            let scope = native_scope(context)?;
            self.directory.require_scope(&scope)?;
            let FileBacking::Native(candidate) = &self.file.backing else {
                unreachable!("control candidates exclusively create native files");
            };
            let mut native = || {
                self.directory.root.with_native(|root| {
                    // NativeDirectoryTrace owns the namespace transaction here.
                    // Reentering the candidate's file trace would relock ModelCore.
                    {
                        let entry = platform::open_child(root, source)?;
                        if crate::native_file_identity::of_file(&entry)?
                            != crate::native_file_identity::of_file(&candidate.inner)?
                        {
                            return Err(io::Error::new(
                                io::ErrorKind::InvalidData,
                                "control candidate entry changed after creation",
                            ));
                        }
                    }
                    {
                        match platform::open_child(root, target) {
                            Ok(entry) => {
                                if !entry.metadata()?.is_file() {
                                    return Err(io::Error::new(
                                        io::ErrorKind::InvalidData,
                                        "control head is not a regular file",
                                    ));
                                }
                                if crate::native_file_identity::of_file(&entry)?
                                    == crate::native_file_identity::of_file(&candidate.inner)?
                                {
                                    return Err(io::Error::new(
                                        io::ErrorKind::AlreadyExists,
                                        "control head aliases the private candidate",
                                    ));
                                }
                            }
                            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
                            Err(error) => return Err(error),
                        }
                    }
                    self.state = State::Attempted;
                    platform::replace_control(root, &candidate.inner, source, target)
                })
            };
            #[cfg(any(test, feature = "test-support"))]
            match self.directory.trace.as_deref() {
                Some(trace) => {
                    trace.replace_file(Path::new(source.0), Path::new(target.0), native)?
                }
                None => native()?,
            }
            #[cfg(not(any(test, feature = "test-support")))]
            native()?;
        }
        drop(_preflight);
        #[cfg(any(test, feature = "test-support"))]
        AFTER_EACH_RENAME.with(|hook| {
            if let Some(callback) = hook.borrow_mut().as_mut() {
                callback()?;
            }
            Ok::<_, io::Error>(())
        })?;
        #[cfg(any(test, feature = "test-support"))]
        AFTER_RENAME.with(|hook| {
            let callback = hook.borrow_mut().take();
            if let Some(callback) = callback {
                callback();
            }
        });
        let mut target = [0; 256];
        target[..name.len()].copy_from_slice(name.as_bytes());
        Ok(PendingControlPublication {
            candidate: self,
            target,
            length: name.len(),
        })
    }

    pub fn is_complete(&self) -> bool {
        self.state == State::Complete
    }

    pub fn selection_attempted(&self) -> bool {
        matches!(self.state, State::Attempted | State::Complete)
    }

    /// Remove only an unselected candidate whose entry still names this held
    /// file. The caller excludes other writers; unknown rename effects retain
    /// their original recovery owner instead of authorizing cleanup.
    pub fn discard_unselected(&mut self, context: &FileOpenContext) -> io::Result<()> {
        if self.state == State::Discarded {
            return self.directory.sync(context);
        }
        if !matches!(self.state, State::Writable | State::Sealed) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "attempted control cannot be discarded",
            ));
        }
        let name = std::str::from_utf8(&self.name[..self.length]).expect("ASCII name");
        if !self
            .directory
            .matches_child_file(context, name, &self.file)?
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "private control entry changed before cleanup",
            ));
        }
        self.directory.remove_child(context, name)?;
        self.state = State::Discarded;
        self.directory.sync(context)
    }

    fn require_writable(&self) -> io::Result<()> {
        if self.state != State::Writable {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "sealed or attempted control candidate is not writable",
            ));
        }
        Ok(())
    }
}

impl PendingControlPublication<'_, '_> {
    /// Complete barriers with a fresh operation context after releasing the
    /// source-owner fence. Errors retain the candidate in attempted state.
    pub fn finish(self, context: &FileOpenContext) -> io::Result<()> {
        self.candidate.directory.require_context(context)?;
        let target = std::str::from_utf8(&self.target[..self.length]).expect("ASCII name");
        let candidate = self.candidate;
        // Separate traced operations preserve completed barriers and lost
        // responses. Never hide a post-rename sync inside the native callback.
        candidate.file.sync_all()?;
        let selected = candidate.directory.open_child(context, target)?;
        if selected.with_native(crate::native_file_identity::of_file)?
            != candidate
                .file
                .with_native(crate::native_file_identity::of_file)?
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "control head differs from the held publication",
            ));
        }
        drop(selected);
        candidate.directory.sync(context)?;
        candidate.state = State::Complete;
        Ok(())
    }
}

impl Write for CapturedControlCandidate<'_> {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.require_writable()?;
        self.file.write(bytes)
    }
    fn flush(&mut self) -> io::Result<()> {
        self.require_writable()?;
        self.file.flush()
    }
}

#[cfg(all(test, any(target_os = "linux", target_vendor = "apple", windows)))]
mod tests;
