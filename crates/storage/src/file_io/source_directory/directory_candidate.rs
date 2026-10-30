// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! Private directory creation evidence survives failure between mkdir and open.

use super::*;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum State {
    Fresh,
    Attempted,
    Refused,
    Created,
    Removed,
}

/// Process-local evidence bound to one held parent capture and one component.
/// The owner must retain that parent, admit this metadata before allocation,
/// and exclude other writers from its private candidate name. This is not a
/// persisted incarnation, a directory lease, or a publication receipt.
///
/// There is no Drop cleanup. In particular, an attempted creation with an
/// uncertain outcome must remain owned for inspection, rather than adopting
/// an arbitrary existing directory or forgetting a possible native effect.
#[doc(hidden)]
#[derive(Debug)]
pub struct CapturedDirectoryCandidate {
    capture_id: u64,
    name: [u8; 256],
    length: usize,
    state: State,
}

impl CapturedDirectoryCandidate {
    pub fn name(&self) -> &str {
        std::str::from_utf8(&self.name[..self.length]).expect("validated ASCII component")
    }

    /// A preflight refusal or an exclusive-create collision owns no entry.
    pub fn may_own_entry(&self) -> bool {
        matches!(self.state, State::Attempted | State::Created)
    }

    pub fn creation_completed(&self) -> bool {
        self.state == State::Created
    }

    pub fn creation_unattempted(&self) -> bool {
        self.state == State::Fresh
    }

    pub fn removal_completed(&self) -> bool {
        self.state == State::Removed
    }
}

impl SourceDirectory {
    /// Prepare fixed-size evidence without I/O, allocation or a native effect.
    pub fn prepare_directory_candidate(
        &self,
        name: &str,
    ) -> io::Result<CapturedDirectoryCandidate> {
        let name = ChildName::new(name)?;
        let mut bytes = [0; 256];
        bytes[..name.0.len()].copy_from_slice(name.0.as_bytes());
        Ok(CapturedDirectoryCandidate {
            capture_id: self.capture_id,
            name: bytes,
            length: name.0.len(),
            state: State::Fresh,
        })
    }

    fn require_candidate(&self, candidate: &CapturedDirectoryCandidate) -> io::Result<()> {
        if candidate.capture_id != self.capture_id {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "directory candidate belongs to another parent capture",
            ));
        }
        Ok(())
    }

    /// Attempt exclusive creation once. The caller-owned evidence is updated
    /// immediately after the native effect, before any post-effect observation
    /// can fail or unwind. Opening the child remains a separate admitted step.
    pub fn create_directory_candidate(
        &self,
        context: &FileOpenContext,
        candidate: &mut CapturedDirectoryCandidate,
    ) -> io::Result<()> {
        let CapturedDirectoryCandidate {
            capture_id,
            name,
            length,
            state,
        } = candidate;
        let component = DirectoryComponent(OsStr::new(
            std::str::from_utf8(&name[..*length]).expect("validated ASCII component"),
        ));
        self.create_directory_evidence(context, *capture_id, component, state)
    }

    pub(super) fn create_directory_evidence(
        &self,
        context: &FileOpenContext,
        capture_id: u64,
        name: DirectoryComponent<'_>,
        state: &mut State,
    ) -> io::Result<()> {
        self.create_directory_evidence_with(context, capture_id, name, state, |root, name| {
            platform::create_directory_component(root, name)
        })
    }

    // Private native collaborator seam; ordinary callers always provide the
    // actual relative mkdir implementation. Tests can fail observation after
    // a real mkdir without introducing a global hook or host permission API.
    pub(super) fn create_directory_evidence_with(
        &self,
        context: &FileOpenContext,
        capture_id: u64,
        name: DirectoryComponent<'_>,
        state: &mut State,
        create_native: impl FnOnce(&std::fs::File, DirectoryComponent<'_>) -> io::Result<()>,
    ) -> io::Result<()> {
        self.require_context(context)?;
        if capture_id != self.capture_id {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "directory candidate belongs to another parent capture",
            ));
        }
        if *state != State::Fresh {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "directory candidate creation was already attempted",
            ));
        }
        let scope = native_scope(context)?;
        self.require_scope(&scope)?;
        let _permit = context.acquire(DescriptorKind::Transient)?;
        let native = || {
            *state = State::Attempted;
            let result = self.root.with_native(|root| create_native(root, name));
            match &result {
                Ok(()) => *state = State::Created,
                Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
                    *state = State::Refused
                }
                Err(_) => {}
            }
            result
        };
        #[cfg(any(test, feature = "test-support"))]
        let result = match self.trace.as_deref() {
            Some(trace) => trace.create_directory(Path::new(name.0), native),
            None => native(),
        };
        #[cfg(not(any(test, feature = "test-support")))]
        let result = native();
        result.map_err(|error| context.map_open_error(error))
    }

    /// Open only a candidate whose native creation completed in this capture.
    /// Unknown creation evidence never authorizes adopting an installed name.
    pub fn open_created_directory(
        &self,
        context: &FileOpenContext,
        candidate: &CapturedDirectoryCandidate,
    ) -> io::Result<Self> {
        self.require_candidate(candidate)?;
        if !candidate.creation_completed() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "directory candidate has no completed creation evidence",
            ));
        }
        self.open_directory(context, candidate.name())
    }

    /// Remove only this caller-owned, empty private directory. The caller must
    /// finish its children and exclude rebinding the private component. Errors
    /// never authorize recursive deletion or adoption of an uncertain entry.
    pub fn remove_created_directory(
        &self,
        context: &FileOpenContext,
        candidate: &mut CapturedDirectoryCandidate,
    ) -> io::Result<()> {
        self.require_context(context)?;
        self.require_candidate(candidate)?;
        if candidate.removal_completed() {
            return Ok(());
        }
        if !candidate.creation_completed() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "cannot remove a directory without completed creation evidence",
            ));
        }
        let scope = native_scope(context)?;
        self.require_scope(&scope)?;
        let CapturedDirectoryCandidate {
            name,
            length,
            state,
            ..
        } = candidate;
        let name = ChildName(std::str::from_utf8(&name[..*length]).expect("validated component"));
        let mut native = || {
            self.root
                .with_native(|root| platform::remove_directory(root, name, context))?;
            *state = State::Removed;
            Ok(())
        };
        #[cfg(any(test, feature = "test-support"))]
        let result = match self.trace.as_deref() {
            Some(trace) => trace.remove_directory(Path::new(name.0), native),
            None => native(),
        };
        #[cfg(not(any(test, feature = "test-support")))]
        let result = native();
        result.map_err(|error| context.map_open_error(error))
    }
}

#[cfg(test)]
mod tests;
