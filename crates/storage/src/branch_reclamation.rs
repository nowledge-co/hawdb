//! Bounded maintenance for a live branch project's immutable storage.

use crate::branch_catalog::{self, BranchReclamationPath, CatalogMetadataLease};
use crate::branch_head::BranchHead;
use crate::error::{HawDBError, Result};
use crate::file_io as fs;
use crate::immutable_object::{BranchReclamationInventory, ImmutableObjectStore, ObjectKind};
use crate::ownership::DatabaseDirectoryLease;
use std::collections::BTreeMap;
use std::path::Path;
use std::time::Instant;

/// Hard limits for a complete inventory. Exceeding either limit retains all
/// objects and returns an error, rather than sweeping a partial inventory.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BranchReclamationLimits {
    pub max_objects: usize,
    pub max_bytes: u64,
}

impl Default for BranchReclamationLimits {
    fn default() -> Self {
        Self {
            max_objects: 100_000,
            max_bytes: 1024 * 1024 * 1024,
        }
    }
}

impl BranchReclamationLimits {
    pub(crate) fn validate(self) -> Result<()> {
        if self.max_objects == 0 || self.max_bytes == 0 {
            return Err(HawDBError::Semantic(
                "branch reclamation limits must be positive".into(),
            ));
        }
        Ok(())
    }
}

/// Evidence from one complete sweep, or a conservative deferral. A deferral
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
}

impl BranchReclamationReport {
    pub(crate) fn deferred() -> Self {
        Self {
            deferred_for_active_leases: true,
            ..Self::default()
        }
    }
}

pub(crate) fn reclaim_owned_project(
    catalog_path: &Path,
    object_root: &Path,
    owner: &DatabaseDirectoryLease,
    head: &BranchHead,
    limits: BranchReclamationLimits,
) -> Result<BranchReclamationReport> {
    let directory = catalog_path.parent().ok_or_else(|| {
        HawDBError::StorageIntegrity("branch catalog has no project directory".into())
    })?;
    // An exclusive borrow of the sole runtime lease proves the owner has no
    // readers or candidates. Metadata serialization excludes new admissions
    // and catalog changes throughout inventory, mark and sweep. An unrelated
    // live owner still defers the entire operation, including other processes.
    let _metadata = CatalogMetadataLease::acquire_blocking(directory)?;
    let started = Instant::now();
    let catalog = branch_catalog::read_catalog(catalog_path)?;
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
    let branches = branch_catalog::reclamation_entries(&catalog, &paths, Some((owner, head)))
        .map_err(HawDBError::from_storage_error)?;
    let mut report = if branches.iter().any(|branch| branch.active_lease) {
        BranchReclamationReport::deferred()
    } else {
        let mut store =
            ImmutableObjectStore::open(object_root).map_err(HawDBError::from_storage_error)?;
        let inventory = store.inventory(limits.max_objects, limits.max_bytes)?;
        let roots: BTreeMap<_, _> = inventory
            .objects
            .iter()
            .filter(|reference| reference.kind == ObjectKind::SealedRoot)
            .map(|reference| (*reference.sha256.as_bytes(), *reference))
            .collect();
        let retained_roots = catalog
            .branches
            .iter()
            .filter(|record| record.state != branch_catalog::BranchState::Deleted)
            .filter_map(|record| record.base_root_digest)
            .map(|digest| {
                roots.get(&digest).copied().ok_or_else(|| {
                    HawDBError::StorageIntegrity(
                        "catalog creation baseline is missing from the immutable inventory".into(),
                    )
                })
            })
            .collect::<Result<Vec<_>>>()?;
        let result = store
            .reclaim_branches_with_roots(
                &BranchReclamationInventory {
                    objects: inventory.objects,
                    branches,
                },
                &retained_roots,
            )
            .map_err(HawDBError::from_storage_error)?;
        let mut staging_bytes = 0;
        for (path, bytes) in &inventory.staging {
            fs::remove_file(path)?;
            crate::durability::sync_parent_directory(path)?;
            staging_bytes += bytes;
        }
        BranchReclamationReport {
            retained_objects: result.retained_objects,
            reclaimed_objects: result.reclaimed_objects,
            reclaimed_bytes: result.reclaimed_bytes.saturating_add(staging_bytes),
            reclaimed_branch_directories: result.reclaimed_branch_directories,
            reclaimed_staging_files: inventory.staging.len() as u64,
            deferred_for_active_leases: result.deferred_for_active_leases,
            metadata_lock_micros: 0,
        }
    };
    report.metadata_lock_micros = u64::try_from(started.elapsed().as_micros()).unwrap_or(u64::MAX);
    Ok(report)
}
