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

//! Bounded maintenance for a live branch project's immutable storage.

use crate::branch_catalog::{self, BranchReclamationPath};
use crate::branch_head::BranchHead;
use crate::error::{HawDBError, Result};
use crate::immutable_object::{ImmutableObjectStore, ObjectSweepBudget};
use crate::ownership::DatabaseDirectoryLease;
use std::io;
use std::path::Path;
use std::time::Instant;

/// Per-pass work budget for one reclamation pass (#1736).
///
/// Liveness is always computed from every retained root; these limits only
/// bound how much unreachable data one pass removes. A larger namespace is
/// swept partially and the remainder is reported as pending for the next
/// pass, instead of refusing the whole pass. An explicit pass also re-hashes
/// up to `max_bytes` of reachable objects as an integrity check; beyond that
/// reachable objects are checked for presence and exact length.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BranchReclamationLimits {
    pub max_objects: usize,
    pub max_bytes: u64,
    /// Historical quarantine verification has a separate budget from the
    /// current object namespace and runs outside publication serialization.
    pub evidence_limits: crate::branch_abandonment::BranchAbandonmentLimits,
}

impl Default for BranchReclamationLimits {
    fn default() -> Self {
        Self {
            max_objects: 100_000,
            max_bytes: 1024 * 1024 * 1024,
            evidence_limits: Default::default(),
        }
    }
}

impl BranchReclamationLimits {
    pub(crate) fn validate(self) -> Result<()> {
        if self.max_objects == 0
            || self.max_bytes == 0
            || self.evidence_limits.max_files == 0
            || self.evidence_limits.max_bytes == 0
        {
            return Err(HawDBError::Semantic(
                "branch reclamation limits must be positive".into(),
            ));
        }
        Ok(())
    }
}

/// Evidence from one reclamation pass, or a conservative deferral. A deferral
/// performs no sweep and does not claim to have inventoried the object store.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct BranchReclamationReport {
    pub retained_objects: u64,
    pub reclaimed_objects: u64,
    pub reclaimed_bytes: u64,
    pub reclaimed_branch_directories: u64,
    pub reclaimed_staging_files: u64,
    pub deferred_for_active_leases: bool,
    pub metadata_lock_micros: u64,
    /// Unreachable objects left for a later pass: beyond this pass's budget,
    /// still being read, or whose removal failed.
    pub pending_objects: u64,
    pub pending_bytes: u64,
    /// A removal or durability barrier failed; retry the pass.
    pub retry_required: bool,
    /// Objects and bytes in the immutable namespace after this pass
    /// (staging excluded). Zero for a deferral.
    pub object_store_objects: u64,
    pub object_store_bytes: u64,
    /// Unreachable objects an automatic pass retained instead of removing:
    /// corrupt ones without quarantine evidence, and quarantined sources
    /// whose archive it could not verify. Not retryable by another pass.
    pub skipped_corrupt_objects: u64,
    pub skipped_quarantine_objects: u64,
    pub skipped_bytes: u64,
    /// The first corrupt unreachable object retained (kind and digest).
    pub corrupt_object: Option<crate::immutable_object::ObjectReference>,
}

impl BranchReclamationReport {
    pub(crate) fn deferred() -> Self {
        Self {
            deferred_for_active_leases: true,
            ..Self::default()
        }
    }
}

/// Who started a pass. Explicit host calls wait for the catalog metadata
/// lock and re-hash reachable objects within budget. Automatic passes after
/// checkpoint publication never wait for a publication, and only check
/// reachable objects for presence, so their cost follows the garbage they
/// remove rather than the live data set.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ReclamationTrigger {
    Explicit,
    Automatic,
}

/// Test faults for the immutable-object sweep (#1736). Each fires once.
#[doc(hidden)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ObjectReclamationFailpoint {
    /// Listing the object namespace fails before anything is removed.
    Listing,
    /// The barrier that makes every retained head and the catalog durable
    /// before the first removal fails.
    DirectorySyncBeforeRemoval,
    /// One object removal fails.
    Remove,
    /// Synchronizing an object directory after its removals fails.
    DirectorySyncAfterRemoval,
}

#[cfg(any(test, feature = "test-support"))]
thread_local! {
    static OBJECT_RECLAMATION_FAILPOINT: std::cell::Cell<Option<ObjectReclamationFailpoint>> =
        const { std::cell::Cell::new(None) };
}

/// Arms one immutable-object sweep fault on this thread.
#[cfg(any(test, feature = "test-support"))]
#[doc(hidden)]
pub fn set_object_reclamation_failpoint(failpoint: Option<ObjectReclamationFailpoint>) {
    OBJECT_RECLAMATION_FAILPOINT.with(|slot| slot.set(failpoint));
}

pub(crate) fn object_reclamation_failpoint(point: ObjectReclamationFailpoint) -> io::Result<()> {
    #[cfg(any(test, feature = "test-support"))]
    if OBJECT_RECLAMATION_FAILPOINT.with(|slot| {
        let fire = slot.get() == Some(point);
        if fire {
            slot.set(None);
        }
        fire
    }) {
        return Err(io::Error::other(format!(
            "injected immutable object reclamation failure: {point:?}"
        )));
    }
    let _ = point;
    Ok(())
}

pub(crate) fn reclaim_owned_project(
    catalog_path: &Path,
    object_root: &Path,
    owner: &DatabaseDirectoryLease,
    head: &BranchHead,
    limits: BranchReclamationLimits,
    trigger: ReclamationTrigger,
) -> Result<BranchReclamationReport> {
    let directory = catalog_path.parent().ok_or_else(|| {
        HawDBError::StorageIntegrity("branch catalog has no project directory".into())
    })?;
    // Registered owners declare historical and prepared roots. Metadata
    // serialization excludes new root registrations and publication throughout
    // the complete mark/sweep; unregistered owners still defer collection.
    let crate::branch_abandonment::ReclamationSnapshot {
        catalog,
        proofs,
        metadata: _metadata,
    } = match trigger {
        ReclamationTrigger::Explicit => {
            crate::branch_abandonment::prepare_reclamation(catalog_path, limits.evidence_limits)?
        }
        ReclamationTrigger::Automatic => crate::branch_abandonment::try_prepare_reclamation(
            catalog_path,
            limits.evidence_limits,
        )?,
    };
    let started = Instant::now();
    if !catalog.branches.iter().any(|record| {
        *record.id.as_uuid().as_bytes() == head.branch_id
            && record.state == branch_catalog::BranchState::Ready
    }) {
        return Err(HawDBError::StorageIntegrity(
            "reclamation owner is no longer a ready catalog branch".into(),
        ));
    }
    let paths: Vec<_> = catalog
        .branches
        .iter()
        .map(|record| {
            let directory = directory.join(record.id.as_uuid().to_string());
            BranchReclamationPath {
                id: record.id,
                head_path: directory.join("branch.head"),
                directory,
            }
        })
        .collect();
    let registered = branch_catalog::reclamation_entries_with_pins(
        &catalog,
        &paths,
        (owner, head),
        limits.max_objects,
        &_metadata,
    )
    .map_err(HawDBError::from)?;
    let branches = registered.branches;
    let mut report = if branches.iter().any(|branch| branch.active_lease) {
        // A head cannot describe unpublished candidates or old reader pins of
        // an unregistered active owner, so its whole namespace is retained.
        BranchReclamationReport::deferred()
    } else {
        let mut store =
            ImmutableObjectStore::open(object_root).map_err(HawDBError::from_storage_error)?;
        let mut retained_roots = Vec::new();
        for record in catalog
            .branches
            .iter()
            .filter(|record| record.state != branch_catalog::BranchState::Deleted)
        {
            let Some(digest) = record.base_root_digest else {
                continue;
            };
            let root =
                store.sealed_root_by_digest(hawdb_integrity::Sha256Digest::from_bytes(digest))?;
            let Some(root) = root else {
                let cause = HawDBError::StorageIntegrity(
                    "catalog creation baseline is missing from the immutable inventory".into(),
                );
                return Err(
                    if record.state == branch_catalog::BranchState::Creating
                        && record.create_outcome == branch_catalog::CreateOutcome::Pending
                    {
                        HawDBError::BranchReclamationBlocked {
                            branch_id: record.id.as_uuid(),
                            catalog_revision: catalog.revision,
                            metadata_revision: record.metadata_revision,
                            cause: Box::new(cause),
                        }
                    } else {
                        cause
                    },
                );
            };
            retained_roots.push(root);
        }
        retained_roots.extend(registered.roots);
        for branch in &branches {
            if !matches!(branch.state, branch_catalog::BranchState::Deleted) {
                retained_roots.push(branch.sealed_root.ok_or_else(|| {
                    HawDBError::StorageIntegrity(
                        "live or recovery-pending branch has no sealed root".into(),
                    )
                })?);
            }
        }
        // Every head read above, and the catalog naming every base root, must
        // be durable before anything they superseded is removed: a head
        // adopted after a crash may still have an unsynchronized rename.
        let mut barrier = || -> io::Result<()> {
            object_reclamation_failpoint(ObjectReclamationFailpoint::DirectorySyncBeforeRemoval)?;
            for branch in &branches {
                if !matches!(branch.state, branch_catalog::BranchState::Deleted) {
                    crate::durability::sync_directory(&branch.directory)?;
                }
            }
            crate::durability::sync_directory(directory)
        };
        let budget = ObjectSweepBudget {
            max_objects: limits.max_objects,
            max_bytes: limits.max_bytes,
            max_verified_reachable_bytes: match trigger {
                ReclamationTrigger::Explicit => limits.max_bytes,
                ReclamationTrigger::Automatic => 0,
            },
            skip_unverifiable: trigger == ReclamationTrigger::Automatic,
        };
        let swept = store
            .sweep_unreachable_bounded(&retained_roots, &proofs, budget, &mut barrier)
            .map_err(|error| {
                HawDBError::from(branch_catalog::reclamation_object_error(
                    &catalog, &branches, error,
                ))
            })?;
        // Retiring a deleted branch directory inspects its quarantined
        // private remnants, which only explicit reclamation verifies.
        let reclaimed_branch_directories =
            if swept.retry_required || trigger == ReclamationTrigger::Automatic {
                0
            } else {
                store
                    .reclaim_deleted_branch_directories(&branches)
                    .map_err(HawDBError::from_storage_error)?
            };
        BranchReclamationReport {
            retained_objects: swept.retained_objects,
            reclaimed_objects: swept.reclaimed_objects,
            reclaimed_bytes: swept
                .reclaimed_bytes
                .saturating_add(swept.reclaimed_staging_bytes),
            reclaimed_branch_directories,
            reclaimed_staging_files: swept.reclaimed_staging_files,
            deferred_for_active_leases: false,
            metadata_lock_micros: 0,
            pending_objects: swept.pending_objects,
            pending_bytes: swept.pending_bytes,
            retry_required: swept.retry_required,
            object_store_objects: swept.store_objects,
            object_store_bytes: swept.store_bytes,
            skipped_corrupt_objects: swept.skipped_corrupt_objects,
            skipped_quarantine_objects: swept.skipped_quarantine_objects,
            skipped_bytes: swept.skipped_bytes,
            corrupt_object: swept.corrupt_object,
        }
    };
    report.metadata_lock_micros = u64::try_from(started.elapsed().as_micros()).unwrap_or(u64::MAX);
    Ok(report)
}
