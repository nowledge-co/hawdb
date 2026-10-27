// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! One-shot, same-parent directory movement through retained native custody.

use super::control_candidate::DirectoryOwner;
use super::*;
use std::sync::Arc;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum State {
    Fresh,
    Refused,
    Attempted,
    NativeMoved,
    Verified,
    Durable,
}

/// A fixed-name relabel retaining the actual original parent and child. The
/// caller separately retains its anchor/namespace exclusion and memory funding
/// through every result, including uncertain effects. Neither capture identity
/// nor an FD context supplies those obligations. Drop closes references only;
/// it never removes, retries, rolls back, or acknowledges a namespace operation.
#[doc(hidden)]
#[derive(Debug)]
#[must_use = "retain original custody and resolve errors before releasing exclusion"]
pub struct CapturedDirectoryRelabel<'a> {
    parent: DirectoryOwner<'a>,
    child: DirectoryOwner<'a>,
    source: [u8; 256],
    target: [u8; 256],
    source_length: usize,
    target_length: usize,
    state: State,
    binding_fault: bool,
}

impl SourceDirectory {
    /// Prepare without I/O or native handle acquisition. Names are portable
    /// ASCII components; only a direct child of this held parent may move.
    /// The caller must fund the returned metadata before preparing it and
    /// exclude source/name mutation during move_once. No authority is minted.
    #[doc(hidden)]
    pub fn prepare_directory_relabel<'a>(
        &'a self,
        original_child: &'a Self,
        source: &str,
        target: &str,
    ) -> io::Result<CapturedDirectoryRelabel<'a>> {
        self.require_context(&original_child.context)?;
        let source = ChildName::new(source)?;
        let target = ChildName::new(target)?;
        if source.0 == target.0 || self.same_directory(original_child) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "directory relabel needs a distinct child and target name",
            ));
        }
        let mut source_bytes = [0; 256];
        let mut target_bytes = [0; 256];
        source_bytes[..source.0.len()].copy_from_slice(source.0.as_bytes());
        target_bytes[..target.0.len()].copy_from_slice(target.0.as_bytes());
        Ok(CapturedDirectoryRelabel {
            parent: DirectoryOwner::Borrowed(self),
            child: DirectoryOwner::Borrowed(original_child),
            source: source_bytes,
            target: target_bytes,
            source_length: source.0.len(),
            target_length: target.0.len(),
            state: State::Fresh,
            binding_fault: false,
        })
    }

    /// Retain existing Arc owners without duplicating original native handles.
    /// The original anchor and actual namespace exclusion remain caller-owned.
    #[doc(hidden)]
    pub fn prepare_shared_directory_relabel(
        parent: Arc<Self>,
        original_child: Arc<Self>,
        source: &str,
        target: &str,
    ) -> io::Result<CapturedDirectoryRelabel<'static>> {
        let candidate = parent.prepare_directory_relabel(&original_child, source, target)?;
        let (source, target, source_length, target_length) = (
            candidate.source,
            candidate.target,
            candidate.source_length,
            candidate.target_length,
        );
        Ok(CapturedDirectoryRelabel {
            parent: DirectoryOwner::Shared(parent),
            child: DirectoryOwner::Shared(original_child),
            source,
            target,
            source_length,
            target_length,
            state: State::Fresh,
            binding_fault: false,
        })
    }

    /// Fixed owner plus bounded diagnostic scratch; this estimate is NOT a
    /// memory permit. Retain actual external funding for the candidate lifetime.
    /// Existing captures, anchor and recorder engine keep their own funding.
    #[doc(hidden)]
    pub fn directory_relabel_metadata_bound(
        &self,
        source_bytes: usize,
        target_bytes: usize,
    ) -> io::Result<usize> {
        if !(1..=255).contains(&source_bytes) || !(1..=255).contains(&target_bytes) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "invalid relabel name length",
            ));
        }
        // Bounded error/observer diagnostics are retained separately from stack scratch.
        let bound = std::mem::size_of::<CapturedDirectoryRelabel<'_>>() + 256;
        #[cfg(any(test, feature = "test-support"))]
        if let Some(trace) = self.trace.as_deref() {
            return bound
                .checked_add(trace.metadata_bound(source_bytes + target_bytes)?)
                .ok_or_else(|| io::Error::other("directory relabel metadata overflow"));
        }
        Ok(bound)
    }
}

impl CapturedDirectoryRelabel<'_> {
    fn source_name(&self) -> ChildName<'_> {
        ChildName(std::str::from_utf8(&self.source[..self.source_length]).expect("ASCII source"))
    }

    fn target_name(&self) -> ChildName<'_> {
        ChildName(std::str::from_utf8(&self.target[..self.target_length]).expect("ASCII target"))
    }

    /// Attempt exactly one exclusive native rename, retaining original custody.
    /// Resource refusal before native entry permits retry. Any native error or
    /// unwind may follow an effect and permanently consumes this move attempt.
    /// Definite success is recorded before any fallible recorder/observer step.
    /// This does not synchronize the parent or grant publication permission.
    pub fn move_once(&mut self, context: &FileOpenContext) -> io::Result<()> {
        self.parent.require_context(context)?;
        self.child.require_context(context)?;
        if self.state != State::Fresh {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "directory relabel already consumed",
            ));
        }
        // The lexical scope drops the helper before its permit and recorder
        // registration, including on unsupported platforms whose File has no
        // Drop implementation. Original child and caller anchor stay held.
        {
            let scope = native_scope(context)?;
            self.parent.require_scope(&scope)?;
            self.child.require_scope(&scope)?;
            // Admit before acquiring a recorder's engine lock. Probes and the
            // DELETE-capable original helper use this one slot sequentially.
            let _permit = context.acquire(DescriptorKind::Transient)?;
            let source = self.source_name();
            let target = self.target_name();
            let prepared = self.parent.root.with_native(|root| {
                if platform::entry_exists(root, target)? {
                    return Err(io::Error::new(
                        io::ErrorKind::AlreadyExists,
                        "directory relabel target occupied",
                    ));
                }
                let child = platform::open_directory_for_relabel(root, source)?;
                if SourceDirectoryIdentity(crate::native_file_identity::of_file(&child)?)
                    != self.child.identity
                {
                    return Err(binding_error());
                }
                Ok(child)
            });
            let helper = match prepared {
                Ok(helper) => helper,
                Err(error) => {
                    let error = context.map_open_error(error);
                    if hawdb_core::error::file_descriptor_error(&error).is_none() {
                        self.state = State::Refused;
                    }
                    return Err(error);
                }
            };
            let parent = &self.parent;
            #[cfg(any(test, feature = "test-support"))]
            let child = &self.child;
            let source = ChildName(
                std::str::from_utf8(&self.source[..self.source_length]).expect("ASCII source"),
            );
            let target = ChildName(
                std::str::from_utf8(&self.target[..self.target_length]).expect("ASCII target"),
            );
            let state = &mut self.state;
            let mut native = || -> io::Result<()> {
                #[cfg(test)]
                hooks::invoke(hooks::Boundary::BeforeNative)?;
                *state = State::Attempted;
                parent.root.with_native(|root| {
                    platform::relabel_directory_new(root, &helper, source, target)
                })?;
                // Private collaborator models a lost native reply AFTER the real
                // syscall, never a fake successful operation. Unknown stays Attempted.
                #[cfg(test)]
                hooks::invoke(hooks::Boundary::NativeReply)?;
                *state = State::NativeMoved;
                Ok(())
            };
            #[cfg(any(test, feature = "test-support"))]
            match (parent.trace.as_deref(), child.trace.as_deref()) {
                (Some(parent), Some(child)) => parent.relabel_directory_new(
                    child,
                    Path::new(source.0),
                    Path::new(target.0),
                    native,
                )?,
                (None, None) => native()?,
                _ => {
                    return Err(io::Error::new(
                        io::ErrorKind::Unsupported,
                        "directory relabel recorder mismatch",
                    ))
                }
            }
            #[cfg(not(any(test, feature = "test-support")))]
            native()?;
        }
        #[cfg(test)]
        hooks::invoke(hooks::Boundary::AfterNative)?;
        self.verify_binding(context)?;
        self.state = State::Verified;
        Ok(())
    }

    fn verify_binding(&mut self, context: &FileOpenContext) -> io::Result<()> {
        if self.binding_fault {
            return Err(binding_error());
        }
        let scope = native_scope(context)?;
        self.parent.require_scope(&scope)?;
        self.child.require_scope(&scope)?;
        // Admission refusal is recoverable and must not become a binding fault.
        let _permit = context.acquire(DescriptorKind::Transient)?;
        let observed = self.parent.root.with_native(|root| {
            if platform::entry_exists(root, self.source_name())? {
                return Ok(false);
            }
            let target = match platform::open_directory(root, self.target_name()) {
                Ok(target) => target,
                Err(error)
                    if matches!(
                        error.kind(),
                        io::ErrorKind::NotFound
                            | io::ErrorKind::NotADirectory
                            | io::ErrorKind::InvalidData
                    ) || no_follow_link_error(&error) =>
                {
                    return Ok(false)
                }
                Err(error) => return Err(error),
            };
            Ok(
                SourceDirectoryIdentity(crate::native_file_identity::of_file(&target)?)
                    == self.child.identity,
            )
        });
        match observed {
            Ok(true) => {}
            Ok(false) => {
                self.binding_fault = true;
                return Err(binding_error());
            }
            // Failure to observe is not proof that the binding changed. In
            // particular, native FD exhaustion must preserve a finish retry.
            Err(error) => return Err(context.map_open_error(error)),
        }
        Ok(())
    }

    /// Freshly validate binding and complete only the held-parent namespace
    /// barrier. FD/sync refusal can retry while originals remain held. An
    /// observed binding fault stays terminal even if the name is restored.
    pub fn finish(&mut self, context: &FileOpenContext) -> io::Result<()> {
        self.parent.require_context(context)?;
        self.child.require_context(context)?;
        if !self.native_move_completed() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "directory relabel has no known native success",
            ));
        }
        self.verify_binding(context)?;
        #[cfg(test)]
        hooks::invoke(hooks::Boundary::BeforeSync)?;
        self.parent.sync(context)?;
        self.state = State::Durable;
        Ok(())
    }

    /// Historical native-entry evidence; never permission to retry or clean up.
    pub fn move_attempted(&self) -> bool {
        matches!(
            self.state,
            State::Attempted | State::NativeMoved | State::Verified | State::Durable
        )
    }
    /// Historical definite native reply, retained through later observation or
    /// binding faults. It does not assert the current namespace still matches.
    pub fn native_move_completed(&self) -> bool {
        matches!(
            self.state,
            State::NativeMoved | State::Verified | State::Durable
        )
    }
    /// Historical uncertainty/effect evidence requiring original recovery custody.
    pub fn may_have_effect(&self) -> bool {
        self.move_attempted()
    }
    /// Historical evidence of at least one successful finish/parent barrier.
    /// Later binding faults do not erase that fact. Only a fresh successful
    /// finish result acknowledges its current validation/barrier; this accessor
    /// grants no current-binding, publication, retirement or reuse permission.
    pub fn durability_completed(&self) -> bool {
        self.state == State::Durable
    }
}

fn no_follow_link_error(error: &io::Error) -> bool {
    #[cfg(unix)]
    {
        error.raw_os_error() == Some(libc::ELOOP)
    }
    #[cfg(not(unix))]
    {
        let _ = error;
        false
    }
}

fn binding_error() -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidData,
        "directory relabel binding changed",
    )
}

#[cfg(test)]
mod hooks {
    use super::*;
    #[derive(Clone, Copy, PartialEq, Eq)]
    pub(super) enum Boundary {
        BeforeNative,
        NativeReply,
        AfterNative,
        BeforeSync,
    }
    type Hook = Option<(Boundary, Box<dyn FnOnce() -> io::Result<()>>)>;
    thread_local! { pub(super) static HOOK: std::cell::RefCell<Hook> = const { std::cell::RefCell::new(None) }; }
    pub(super) fn invoke(boundary: Boundary) -> io::Result<()> {
        let callback = HOOK.with(|hook| {
            let mut hook = hook.borrow_mut();
            if hook.as_ref().is_some_and(|(point, _)| *point == boundary) {
                hook.take().map(|(_, callback)| callback)
            } else {
                None
            }
        });
        if let Some(callback) = callback {
            callback()?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests;
