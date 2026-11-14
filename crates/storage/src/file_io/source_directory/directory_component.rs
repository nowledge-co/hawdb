// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! Configured directory names retain native OS spelling. Artifact names keep
//! their separate portable ASCII contract; neither name grants publication or
//! cleanup authority.
//!
//! Windows operations use verbatim NT components. Acceptance does not guarantee
//! ordinary Win32 pathname or Explorer interoperability: trailing dots/spaces
//! and DOS device names can have different meanings there. Before effects,
//! configured-root callers must either validate names for every pathname API
//! they will use or retain native-component operations through reopen and
//! cleanup. This primitive does not choose or qualify that caller policy.

use super::{directory_candidate::State, *};
use std::ffi::OsString;

/// Creation evidence tied to one actual captured parent and one native name.
/// The caller owns namespace exclusion separately. No Drop cleanup is done;
/// an uncertain creation is retained rather than retried or adopted by name.
#[doc(hidden)]
#[derive(Debug)]
pub struct CapturedDirectoryComponentCandidate {
    capture_id: u64,
    name: OsString,
    state: State,
}

impl CapturedDirectoryComponentCandidate {
    pub fn name(&self) -> &OsStr {
        &self.name
    }

    pub fn may_own_entry(&self) -> bool {
        matches!(self.state, State::Attempted | State::Created)
    }

    pub fn creation_completed(&self) -> bool {
        self.state == State::Created
    }

    pub fn creation_unattempted(&self) -> bool {
        self.state == State::Fresh
    }
}

impl SourceDirectory {
    /// Conservative retained metadata for a native-component capture. Validate
    /// and reserve this bound before opening; retain the caller's memory lease
    /// alongside the returned directory until its trace has dropped. This is an
    /// estimate, not internal memory admission. Shared model state is excluded.
    pub fn native_component_metadata_bound(&self, name: &OsStr) -> io::Result<usize> {
        let name = DirectoryComponent::new(name)?;
        #[cfg(all(target_vendor = "apple", any(test, feature = "test-support")))]
        if self.trace.is_some() {
            // A native alias may resolve an original registered spelling longer
            // than the requested spelling. Admit before that trace allocation.
            return self.metadata_bound(libc::PATH_MAX as usize - 1);
        }
        self.metadata_bound(name.0.len())
    }

    /// Open one existing native component relative to the actual held parent.
    /// No parent pathname is reopened, no missing entry is created, and child
    /// symlinks/reparse points are rejected. I/O admission and namespace
    /// exclusion remain caller obligations.
    pub fn open_directory_component(
        &self,
        context: &FileOpenContext,
        name: &OsStr,
    ) -> io::Result<Self> {
        self.require_context(context)?;
        let name = DirectoryComponent::new(name)?;
        #[cfg(all(target_vendor = "apple", any(test, feature = "test-support")))]
        if let Some(trace) = self.trace.as_deref() {
            let native = native_scope(context)?;
            self.require_scope(&native)?;
            let permit = context.acquire(DescriptorKind::Transient)?;
            let open = |name: &OsStr| {
                self.root
                    .with_native(|root| {
                        platform::open_directory_component(root, DirectoryComponent::new(name)?)
                    })
                    .map_err(|error| context.map_open_error(error))
            };
            let inner = open(name.0)?;
            let trace = trace.open_directory_component(Path::new(name.0), context, &inner, open)?;
            return Self::finish_directory_capture(context, inner, permit, Some(Box::new(trace)));
        }
        self.open_directory_with(context, Path::new(name.0), |root| {
            platform::open_directory_component(root, name)
        })
    }

    /// Validate and retain one native component without I/O. The original
    /// parent capture must remain live; copying a spelling cannot mint a birth.
    pub fn prepare_directory_component_candidate(
        &self,
        name: &OsStr,
    ) -> io::Result<CapturedDirectoryComponentCandidate> {
        let name = DirectoryComponent::new(name)?;
        let mut owned = OsString::new();
        owned
            .try_reserve(name.0.len())
            .map_err(|error| io::Error::new(io::ErrorKind::OutOfMemory, error))?;
        owned.push(name.0);
        Ok(CapturedDirectoryComponentCandidate {
            capture_id: self.capture_id,
            name: owned,
            state: State::Fresh,
        })
    }

    /// Exclusively attempt this candidate once. Completed native creation is
    /// recorded before post-effect recorder observations can fail. An occupied
    /// name is never adopted; any uncertain outcome stays retained for the
    /// caller's recovery. Opening and namespace durability are separate steps.
    pub fn create_directory_component_candidate(
        &self,
        context: &FileOpenContext,
        candidate: &mut CapturedDirectoryComponentCandidate,
    ) -> io::Result<()> {
        let CapturedDirectoryComponentCandidate {
            capture_id,
            name,
            state,
        } = candidate;
        let name = DirectoryComponent::new(name)?;
        self.create_directory_evidence(context, *capture_id, name, state)
    }

    /// Capture only a candidate whose native creation completed in this exact
    /// parent capture. A later descriptor/open failure preserves its evidence.
    /// The caller must still exclude rebinding of the private name.
    pub fn open_created_directory_component(
        &self,
        context: &FileOpenContext,
        candidate: &CapturedDirectoryComponentCandidate,
    ) -> io::Result<Self> {
        if candidate.capture_id != self.capture_id || !candidate.creation_completed() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "directory component has no completed creation in this parent capture",
            ));
        }
        self.open_directory_component(context, candidate.name())
    }
}

#[cfg(all(test, any(unix, windows)))]
mod tests;
