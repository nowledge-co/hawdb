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

//! Bounded recovery of interrupted child creation without admitting main.

use crate::branch_catalog::{
    self, BranchCreateError, BranchId, CreateOutcome, CreateRecoveryOutcome,
};
use crate::branch_head::{BranchHead, BranchHeadError};
use crate::branch_project::{catalog_path, ProjectMetadata};
use crate::file_io as fs;
use crate::immutable_object::ImmutableObjectStore;
use crate::ownership::DatabaseDirectoryLeaseError;
use crate::sealed_root::SealedRoot;
use hawdb_core::{HawDBError, Result, Uuid};
use std::io;
use std::path::Path;

/// Aggregate work admitted by one scan. Repeated reads count separately.
/// Catalog decoding retains its existing independent 16 MiB limit.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BranchCreateRecoveryLimits {
    pub max_creates: usize,
    pub max_files: usize,
    pub max_bytes: u64,
}

impl Default for BranchCreateRecoveryLimits {
    fn default() -> Self {
        Self {
            max_creates: 64,
            max_files: 10_000,
            max_bytes: 256 * 1024 * 1024,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BranchCreateRecoveryStatus {
    Completed,
    Aborted,
    Busy,
    Retained,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BranchCreateRecoveryEntry {
    pub branch_id: Uuid,
    pub status: BranchCreateRecoveryStatus,
    pub error: Option<String>,
}

/// Per-receipt results from a writable open or explicit retry. A retained or
/// unattempted receipt is not ready. Healthy branches remain independently
/// admissible, and the caller can inspect every incomplete recovery outcome.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct BranchCreateRecoveryReport {
    pub pending_at_start: usize,
    pub entries: Vec<BranchCreateRecoveryEntry>,
    pub unattempted: usize,
    pub admitted_files: usize,
    pub admitted_bytes: u64,
    pub limit_exceeded: bool,
}

struct Budget {
    limits: BranchCreateRecoveryLimits,
    files: usize,
    bytes: u64,
    exceeded: bool,
}

impl Budget {
    fn new(limits: BranchCreateRecoveryLimits) -> io::Result<Self> {
        if limits.max_creates == 0 || limits.max_files == 0 || limits.max_bytes == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "pending branch recovery limits must be positive",
            ));
        }
        Ok(Self {
            limits,
            files: 0,
            bytes: 0,
            exceeded: false,
        })
    }

    fn admit(&mut self, bytes: u64) -> io::Result<()> {
        let total = self.bytes.checked_add(bytes);
        if self.files >= self.limits.max_files
            || total.is_none_or(|total| total > self.limits.max_bytes)
        {
            self.exceeded = true;
            return Err(io::Error::other("pending branch recovery budget exceeded"));
        }
        self.files += 1;
        self.bytes = total.expect("checked byte total");
        Ok(())
    }

    fn admit_file(&mut self, path: &Path) -> io::Result<()> {
        let bytes = match fs::symlink_metadata(path) {
            Ok(metadata) if metadata.is_file() => metadata.len(),
            Ok(_) => {
                return Err(io::Error::other(
                    "pending branch dependency is not a regular file",
                ))
            }
            // The receipt kernel decides whether a known missing pair aborts.
            Err(error) if error.kind() == io::ErrorKind::NotFound => 0,
            Err(error) => return Err(error),
        };
        self.admit(bytes)
    }
}

/// Recover one matching request with the same closure validation as startup.
/// The private WAL additionally obeys the caller's existing replay byte cap.
pub fn recover_create_from_head_file(
    catalog_path: &Path,
    branch_id: BranchId,
    head_path: &Path,
    max_active_wal_bytes: u64,
    limits: BranchCreateRecoveryLimits,
) -> std::result::Result<CreateRecoveryOutcome, BranchCreateError> {
    let mut budget = Budget::new(limits).map_err(dependency_error)?;
    let directory = catalog_path
        .parent()
        .ok_or(BranchCreateError::InconsistentRequest(
            "catalog has no directory",
        ))?;
    let objects = ImmutableObjectStore::open_existing(directory.join("objects"), limits.max_bytes);
    recover_one(
        catalog_path,
        branch_id,
        head_path,
        max_active_wal_bytes,
        &objects,
        &mut budget,
    )
}

fn recover_one(
    catalog_path: &Path,
    branch_id: BranchId,
    head_path: &Path,
    max_active_wal_bytes: u64,
    objects: &ImmutableObjectStore,
    budget: &mut Budget,
) -> std::result::Result<CreateRecoveryOutcome, BranchCreateError> {
    let remaining = budget.limits.max_bytes.saturating_sub(budget.bytes);
    branch_catalog::recover_create_with_validation(
        catalog_path,
        branch_id,
        head_path,
        max_active_wal_bytes.min(remaining),
        |head, path| {
            budget.admit_file(path).map_err(dependency_error)?;
            if let Some(head) = head {
                verify_dependencies(objects, head, budget).map_err(dependency_error)?;
            }
            Ok(())
        },
    )
}

impl ProjectMetadata {
    /// Repair only pending child receipts. No main runtime is admitted. Busy
    /// or invalid children retain their evidence and appear in the report.
    pub fn recover_pending_creates(
        &self,
        limits: BranchCreateRecoveryLimits,
    ) -> Result<BranchCreateRecoveryReport> {
        let mut budget =
            Budget::new(limits).map_err(|error| HawDBError::Semantic(error.to_string()))?;
        let pending: Vec<_> = self
            .catalog()
            .branches
            .iter()
            .filter(|branch| branch.create_outcome == CreateOutcome::Pending)
            .map(|branch| branch.id)
            .collect();
        let mut report = BranchCreateRecoveryReport {
            pending_at_start: pending.len(),
            ..Default::default()
        };
        let root = self.file_descriptors().root();
        let objects =
            ImmutableObjectStore::open_existing(root.join("branches/objects"), limits.max_bytes);
        for id in pending.into_iter().take(limits.max_creates) {
            let head_path = root
                .join("branches")
                .join(id.as_uuid().to_string())
                .join("branch.head");
            let outcome = recover_one(
                &catalog_path(root),
                id,
                &head_path,
                limits.max_bytes,
                &objects,
                &mut budget,
            );
            let (status, error) = match outcome {
                Ok(CreateRecoveryOutcome::Completed) => {
                    (BranchCreateRecoveryStatus::Completed, None)
                }
                Ok(CreateRecoveryOutcome::Aborted) => (BranchCreateRecoveryStatus::Aborted, None),
                Err(BranchCreateError::Lease(DatabaseDirectoryLeaseError::AlreadyOpen)) => {
                    (BranchCreateRecoveryStatus::Busy, None)
                }
                Err(error) => (
                    BranchCreateRecoveryStatus::Retained,
                    Some(error.to_string()),
                ),
            };
            report.entries.push(BranchCreateRecoveryEntry {
                branch_id: id.as_uuid(),
                status,
                error,
            });
            if budget.exceeded {
                break;
            }
        }
        report.unattempted = report.pending_at_start - report.entries.len();
        report.admitted_files = budget.files;
        report.admitted_bytes = budget.bytes;
        report.limit_exceeded = budget.exceeded || report.unattempted != 0;
        Ok(report)
    }
}

fn verify_dependencies(
    objects: &ImmutableObjectStore,
    head: &BranchHead,
    budget: &mut Budget,
) -> io::Result<()> {
    // Sealed-root metadata is decoded in memory; its payload has a fixed
    // decoder ceiling. Check it before the object reader allocates a buffer.
    if head.sealed_root.byte_length > 64 * 1024 * 1024 {
        return Err(io::Error::other(
            "pending branch sealed root exceeds its metadata limit",
        ));
    }
    budget.admit(head.sealed_root.byte_length)?;
    let bytes = objects.read(head.sealed_root).map_err(io::Error::other)?;
    let sealed = SealedRoot::decode(&bytes).map_err(io::Error::other)?;
    if sealed.commit_epoch != head.logical_commit_epoch
        || sealed.replay_end_lsn() != head.active_wal.replay_start_lsn
    {
        return Err(io::Error::other(
            "pending branch sealed root does not match its head",
        ));
    }
    drop(bytes);
    for reference in std::iter::once(sealed.durable_manifest)
        .chain(sealed.checkpoint_references)
        .chain(sealed.sealed_wals.into_iter().map(|wal| wal.object))
        .chain(std::iter::once(head.sealed_root))
    {
        budget.admit(reference.byte_length)?;
        objects
            .verify_and_sync(reference)
            .map_err(io::Error::other)?;
    }
    Ok(())
}

fn dependency_error(source: io::Error) -> BranchCreateError {
    BranchCreateError::Head(BranchHeadError::Io {
        operation: "validate pending branch dependencies",
        source,
    })
}
