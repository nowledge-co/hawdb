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

use super::doctor::DatabaseDoctor;
use super::{
    file_checksum, load_published_canonical_adjacency, load_published_property_projection,
    store_id_for_path, DurableManifest, GraphManifestOpenBudget, GraphStore, RecoveryMode,
    SegmentCache, StorageResidencyMode, WalReplayConfig, MANIFEST_FILE,
};
use crate::error::{HawDBError, Result};
use hawdb_storage::derived_repair::{plan_identity, validate_options, validate_plan};
pub use hawdb_storage::derived_repair::{
    DerivedArtifactBranchSource, DerivedArtifactHealth, DerivedArtifactHealthReport,
    DerivedArtifactHealthState, DerivedArtifactKind, DerivedArtifactRebuildOptions,
    DerivedArtifactRepairPlan, DerivedArtifactRepairReport, DERIVED_ARTIFACT_REPAIR_PROTOCOL,
};
use std::path::{Path, PathBuf};
use std::sync::Arc;

#[path = "derived_repair/audit.rs"]
#[doc(hidden)]
pub mod audit;
#[path = "derived_repair/publication.rs"]
pub(super) mod publication;
use audit::{
    finalize_repair, load_matching_pending_record, load_single_pending_record, prepare_repair,
    validate_pending_record,
};

struct DerivedInspection {
    store: GraphStore,
    recovered_catalog: crate::schema::Catalog,
    health: DerivedArtifactHealthReport,
    manifest: DurableManifest,
}

impl DatabaseDoctor {
    pub fn derived_artifact_health(
        path: impl AsRef<Path>,
        options: DerivedArtifactRebuildOptions,
    ) -> Result<DerivedArtifactHealthReport> {
        validate_options(options)?;
        let directory = super::doctor::repair_directory(path.as_ref())?;
        let path = directory.as_path();
        let _files = super::doctor::repair_file_descriptors(path)?;
        if let Some(record) = load_single_pending_record(path)? {
            validate_pending_record(path, &record)?;
            let inspection = inspect(path, record.plan.options)?;
            return Ok(inspection.health);
        }
        Ok(inspect(path, options)?.health)
    }

    pub fn plan_derived_artifact_rebuild(
        path: impl AsRef<Path>,
        options: DerivedArtifactRebuildOptions,
    ) -> Result<DerivedArtifactRepairPlan> {
        validate_options(options)?;
        let directory = super::doctor::repair_directory(path.as_ref())?;
        let path = directory.as_path();
        let _files = super::doctor::repair_file_descriptors(path)?;
        if let Some(record) = load_single_pending_record(path)? {
            validate_pending_record(path, &record)?;
            return Ok(record.plan);
        }
        let inspection = inspect(path, options)?;
        plan_from_inspection(&inspection, options)
    }

    pub fn apply_derived_artifact_rebuild(
        path: impl AsRef<Path>,
        plan: &DerivedArtifactRepairPlan,
    ) -> Result<DerivedArtifactRepairReport> {
        validate_plan(plan)?;
        let directory = super::doctor::repair_directory(path.as_ref())?;
        let path = directory.as_path();
        let _files = super::doctor::repair_file_descriptors(path)?;
        let pending = load_matching_pending_record(path, &plan.plan_id)?;
        if let Some(record) = &pending {
            validate_pending_record(path, record)?;
            if record.plan != *plan {
                return Err(HawDBError::Storage(
                    "pending derived repair plan differs from the requested plan".into(),
                ));
            }
        }
        let mut inspection = inspect(path, plan.options)?;
        if let Some(record) = &pending
            && inspection.manifest.checkpoint_epoch == record.plan.target_generation
            && inspection.manifest.checkpoint_commit_epoch == plan.source_commit_epoch
            && inspection.store.commit_epoch == plan.source_commit_epoch
            && !inspection.health.repair_required
        {
            // Keep the UUID lease through validation and audit completion.
            return finalize_repair(path, record.clone(), true);
        }
        if pending.is_none() && plan_from_inspection(&inspection, plan.options)? != *plan {
            return Err(HawDBError::Storage(
                "derived artifact repair plan no longer matches the current database state"
                    .to_string(),
            ));
        }
        validate_source_identity(path, plan)?;
        if inspection.manifest.checkpoint_epoch != plan.source_generation
            || inspection.store.commit_epoch != plan.source_commit_epoch
            || inspection.health.source_node_count != plan.source_node_count
            || inspection.health.source_relationship_count != plan.source_relationship_count
            || inspection.health.source_logical_bytes != plan.source_logical_bytes
            || inspection
                .store
                .durable
                .as_ref()
                .unwrap()
                .next_checkpoint_generation()?
                != plan.target_generation
        {
            return Err(HawDBError::Storage(
                "derived repair source no longer matches the planned recovery".into(),
            ));
        }
        let resumed = pending.is_some();
        let prepared = match pending {
            Some(record) => record,
            None => prepare_repair(path, plan)?,
        };
        validate_pending_record(path, &prepared)?;
        if resumed && plan.branch.is_some() {
            audit::archive_unpublished_checkpoint(
                path,
                &prepared,
                inspection.store.durable.as_ref().unwrap().root_path(),
            )?;
        }
        if plan.branch.is_some() {
            inspection
                .store
                .authorize_derived_repair_publication(Arc::new(
                    publication::DerivedRepairPublication::from_prepared(path, prepared.clone()),
                ))?;
        }
        inspection.store.enable_derived_repair_writes()?;
        let build_config = build_config(plan.options)?;
        inspection
            .store
            .checkpoint_with_reader_epoch_and_build_config(
                &inspection.recovered_catalog,
                None,
                build_config,
            )?;
        let durable = inspection.store.durable.as_ref().unwrap();
        let manifest = match &plan.branch {
            Some(source) => DurableManifest::load(&published_branch(path, source)?.manifest_path)?,
            None => DurableManifest::load(durable.manifest_path())?,
        };
        if manifest.checkpoint_epoch != plan.target_generation
            || manifest.checkpoint_commit_epoch != plan.source_commit_epoch
            || assess_artifacts(
                durable.root_path(),
                &inspection.store,
                manifest,
                plan.options,
            )?
            .iter()
            .any(|artifact| artifact.state == DerivedArtifactHealthState::RepairRequired)
        {
            return Err(HawDBError::Storage(
                "derived artifact rebuild did not publish one healthy target generation; pending audit was retained"
                    .to_string(),
            ));
        }
        let prepared = if plan.branch.is_some() {
            load_matching_pending_record(path, &plan.plan_id)?.ok_or_else(|| {
                HawDBError::Storage("published branch repair lost its pending audit".into())
            })?
        } else {
            prepared
        };
        finalize_repair(path, prepared, resumed)
    }
}

pub(crate) fn reject_pending_derived_artifact_repair(path: &Path) -> Result<()> {
    let pending = audit::pending_record_paths_at_directory(path)?;
    if pending.is_empty() {
        return Ok(());
    }
    Err(HawDBError::Storage(format!(
        "database has {} interrupted derived artifact repair record(s); finish the repair with DatabaseDoctor before opening the database",
        pending.len()
    )))
}

fn inspect(path: &Path, options: DerivedArtifactRebuildOptions) -> Result<DerivedInspection> {
    let replay = WalReplayConfig {
        recovery_mode: RecoveryMode::Strict,
        max_entries: Some(options.max_wal_replay_entries),
        max_bytes: Some(options.max_wal_replay_bytes),
        segment_cache_capacity_bytes: options.segment_cache_capacity_bytes,
        max_graph_manifest_open_bytes: options.max_graph_manifest_open_bytes,
        residency_mode: StorageResidencyMode::OutOfCore,
        max_out_of_core_delta_bytes: Some(options.max_source_logical_bytes),
        ..WalReplayConfig::default()
    };
    let (store, recovered_catalog) = GraphStore::open_for_derived_repair(path, replay)?;
    let durable = store.durable.as_ref().expect("repair source is durable");
    let manifest = DurableManifest::load(durable.manifest_path())?;
    manifest.validate()?;
    let (node_count, relationship_count, logical_bytes) =
        validate_canonical_source(&store, options)?;
    let artifacts = assess_artifacts(durable.root_path(), &store, manifest, options)?;
    let repair_required = artifacts
        .iter()
        .any(|artifact| artifact.state == DerivedArtifactHealthState::RepairRequired);
    Ok(DerivedInspection {
        store,
        recovered_catalog,
        health: DerivedArtifactHealthReport {
            protocol: DERIVED_ARTIFACT_REPAIR_PROTOCOL.to_string(),
            checkpoint_generation: manifest.checkpoint_epoch,
            checkpoint_commit_epoch: manifest.checkpoint_commit_epoch,
            canonical_source_validated: true,
            source_node_count: node_count,
            source_relationship_count: relationship_count,
            source_logical_bytes: logical_bytes,
            artifacts,
            repair_required,
        },
        manifest,
    })
}

fn validate_canonical_source(
    store: &GraphStore,
    options: DerivedArtifactRebuildOptions,
) -> Result<(u64, u64, u64)> {
    let logical_bytes = store.estimated_logical_record_bytes();
    if logical_bytes > options.max_source_logical_bytes {
        return Err(HawDBError::Storage(format!(
            "derived repair source byte admission rejected {logical_bytes} bytes under the {} byte limit",
            options.max_source_logical_bytes
        )));
    }
    let mut node_count = 0u64;
    for node in store.node_records_owned() {
        node.map_err(|error| HawDBError::StorageIntegrity(error.to_string()))?;
        node_count = node_count.saturating_add(1);
        if node_count > options.max_source_records {
            return Err(HawDBError::Storage(format!(
                "derived repair source record admission exceeded {} records",
                options.max_source_records
            )));
        }
    }
    let mut relationship_count = 0u64;
    for relationship in store.relationship_records_owned() {
        relationship.map_err(|error| HawDBError::StorageIntegrity(error.to_string()))?;
        relationship_count = relationship_count.saturating_add(1);
        if node_count.saturating_add(relationship_count) > options.max_source_records {
            return Err(HawDBError::Storage(format!(
                "derived repair source record admission exceeded {} records",
                options.max_source_records
            )));
        }
    }
    Ok((node_count, relationship_count, logical_bytes))
}

fn assess_artifacts(
    path: &Path,
    store: &GraphStore,
    manifest: DurableManifest,
    options: DerivedArtifactRebuildOptions,
) -> Result<Vec<DerivedArtifactHealth>> {
    if manifest.checkpoint_generation.is_none() {
        return Ok(Vec::new());
    }
    let canonical_relationship_count = store
        .canonical_base
        .as_ref()
        .map(|reader| reader.manifest().relationship_count)
        .unwrap_or_default();
    Ok(vec![
        assess_one(DerivedArtifactKind::CanonicalAdjacency, || {
            validate_branch_derived_objects(store, DerivedArtifactKind::CanonicalAdjacency)?;
            let cache = Arc::new(SegmentCache::new(options.segment_cache_capacity_bytes));
            let mut open_budget =
                GraphManifestOpenBudget::new(options.max_graph_manifest_open_bytes);
            let reader = load_published_canonical_adjacency(
                path,
                manifest,
                cache,
                store_id_for_path(path)?,
                &mut open_budget,
            )?
            .ok_or_else(|| {
                HawDBError::Storage("canonical adjacency publication is missing".to_string())
            })?;
            if reader.relationship_count() != canonical_relationship_count {
                return Err(HawDBError::Storage(
                    "canonical adjacency relationship count mismatch".to_string(),
                ));
            }
            reader
                .deep_scrub()
                .map(|_| ())
                .map_err(|error| HawDBError::StorageIntegrity(error.to_string()))
        })?,
        assess_one(DerivedArtifactKind::PersistentPropertyProjection, || {
            validate_branch_derived_objects(
                store,
                DerivedArtifactKind::PersistentPropertyProjection,
            )?;
            let cache = Arc::new(SegmentCache::new(options.segment_cache_capacity_bytes));
            let mut open_budget =
                GraphManifestOpenBudget::new(options.max_graph_manifest_open_bytes);
            let reader = load_published_property_projection(
                path,
                manifest,
                cache,
                store_id_for_path(path)?,
                &mut open_budget,
            )?
            .ok_or_else(|| {
                HawDBError::Storage(
                    "persistent property projection publication is missing".to_string(),
                )
            })?;
            reader
                .deep_scrub()
                .map(|_| ())
                .map_err(|error| HawDBError::StorageIntegrity(error.to_string()))
        })?,
    ])
}

pub(super) fn rebuildable_artifact_kind(
    path: &str,
    generation: u64,
) -> Option<DerivedArtifactKind> {
    [
        DerivedArtifactKind::CanonicalAdjacency,
        DerivedArtifactKind::PersistentPropertyProjection,
    ]
    .into_iter()
    .find(|kind| {
        audit::target_files(*kind, generation)
            .iter()
            .any(|name| name == path)
    })
}

fn validate_branch_derived_objects(store: &GraphStore, kind: DerivedArtifactKind) -> Result<()> {
    let durable = store.durable.as_ref().expect("repair source is durable");
    let Some(branch) = &durable.branch_runtime else {
        return Ok(());
    };
    let objects =
        crate::immutable_object::ImmutableObjectStore::open(branch.immutable_store_root())
            .map_err(HawDBError::from_storage_error)?;
    let references = branch
        .root()
        .checkpoint_bindings
        .iter()
        .filter(|binding| {
            rebuildable_artifact_kind(&binding.relative_path, durable.checkpoint_epoch)
                == Some(kind)
        })
        .map(|binding| binding.reference)
        .collect::<std::collections::BTreeSet<_>>();
    for reference in references {
        objects
            .read(reference)
            .map_err(HawDBError::from_storage_error)?;
    }
    Ok(())
}

fn assess_one(
    kind: DerivedArtifactKind,
    check: impl FnOnce() -> Result<()>,
) -> Result<DerivedArtifactHealth> {
    match check() {
        Ok(()) => Ok(DerivedArtifactHealth {
            kind,
            state: DerivedArtifactHealthState::Healthy,
            reason_code: None,
        }),
        Err(error @ HawDBError::FileDescriptors(_)) => Err(error),
        Err(_) => Ok(DerivedArtifactHealth {
            kind,
            state: DerivedArtifactHealthState::RepairRequired,
            reason_code: Some("artifact_missing_corrupt_or_inconsistent".to_string()),
        }),
    }
}

fn plan_from_inspection(
    inspection: &DerivedInspection,
    options: DerivedArtifactRebuildOptions,
) -> Result<DerivedArtifactRepairPlan> {
    let targets = inspection
        .health
        .artifacts
        .iter()
        .filter(|artifact| artifact.state == DerivedArtifactHealthState::RepairRequired)
        .map(|artifact| artifact.kind)
        .collect::<Vec<_>>();
    if targets.is_empty() {
        return Err(HawDBError::Storage(
            "derived artifact doctor found no repair-required artifact".to_string(),
        ));
    }
    let estimated_temporary_bytes = inspection
        .health
        .source_logical_bytes
        .saturating_mul(8)
        .max(64 * 1024 * 1024);
    if estimated_temporary_bytes > options.max_temporary_bytes {
        return Err(HawDBError::Storage(format!(
            "derived repair temporary byte admission rejected {estimated_temporary_bytes} estimated bytes under the {} byte limit",
            options.max_temporary_bytes
        )));
    }
    let durable = inspection
        .store
        .durable
        .as_ref()
        .expect("repair source is durable");
    let manifest_identity = file_checksum(durable.manifest_path())?;
    let wal_identity = file_checksum(&durable.wal_path)?;
    let branch = durable
        .branch_runtime
        .as_ref()
        .map(|branch| {
            let head = file_checksum(branch.head_path())?;
            Ok::<_, HawDBError>(DerivedArtifactBranchSource {
                project_id: hawdb_core::Uuid::from_bytes(branch.head.project_id),
                branch_id: hawdb_core::Uuid::from_bytes(branch.head.branch_id),
                metadata_revision: branch.metadata_revision(),
                head_len: head.0,
                head_crc32c: head.1,
                head_sha256: head.2.to_string(),
            })
        })
        .transpose()?;
    let mut plan = DerivedArtifactRepairPlan {
        protocol: DERIVED_ARTIFACT_REPAIR_PROTOCOL.to_string(),
        plan_id: String::new(),
        branch,
        source_generation: inspection.manifest.checkpoint_epoch,
        target_generation: durable.next_checkpoint_generation()?,
        source_commit_epoch: inspection.store.commit_epoch,
        manifest_len: manifest_identity.0,
        manifest_crc32c: manifest_identity.1,
        manifest_sha256: manifest_identity.2.to_string(),
        wal_len: wal_identity.0,
        wal_crc32c: wal_identity.1,
        wal_sha256: wal_identity.2.to_string(),
        source_node_count: inspection.health.source_node_count,
        source_relationship_count: inspection.health.source_relationship_count,
        source_logical_bytes: inspection.health.source_logical_bytes,
        estimated_temporary_bytes,
        targets,
        options,
    };
    plan.plan_id = plan_identity(&plan);
    Ok(plan)
}

pub(crate) use hawdb_storage::derived_repair::build_config;

fn validate_source_identity(path: &Path, plan: &DerivedArtifactRepairPlan) -> Result<()> {
    let (manifest_path, wal_path) = match &plan.branch {
        Some(source) => {
            let published = published_branch(path, source)?;
            let head = file_checksum(&path.join("branch.head"))?;
            if head.0 != source.head_len
                || head.1 != source.head_crc32c
                || head.2.to_string() != source.head_sha256
            {
                return Err(HawDBError::Storage(
                    "derived artifact repair branch head changed after planning".into(),
                ));
            }
            (published.manifest_path, published.wal_path)
        }
        None => {
            let manifest_path = path.join(MANIFEST_FILE);
            let manifest = DurableManifest::load(&manifest_path)?;
            (manifest_path, manifest.wal_path(path))
        }
    };
    let manifest = file_checksum(&manifest_path)?;
    let wal = file_checksum(&wal_path)?;
    if manifest.0 != plan.manifest_len
        || manifest.1 != plan.manifest_crc32c
        || manifest.2.to_string() != plan.manifest_sha256
        || wal.0 != plan.wal_len
        || wal.1 != plan.wal_crc32c
        || wal.2.to_string() != plan.wal_sha256
    {
        return Err(HawDBError::Storage(
            "derived artifact repair source identity changed after planning".to_string(),
        ));
    }
    Ok(())
}

struct PublishedRepairBranch {
    head: crate::branch_head::BranchHead,
    root: crate::sealed_root::SealedRoot,
    objects: crate::immutable_object::ImmutableObjectStore,
    manifest_path: PathBuf,
    wal_path: PathBuf,
}

/// Resolves only the durable selector and its metadata objects. Disposable
/// runtime manifests never decide whether a branch repair was committed.
fn published_branch(
    path: &Path,
    source: &DerivedArtifactBranchSource,
) -> Result<PublishedRepairBranch> {
    let id = source.branch_id.to_string();
    let branches = path
        .parent()
        .filter(|parent| {
            parent.file_name().is_some_and(|name| name == "branches")
                && path.file_name().is_some_and(|name| name == id.as_str())
        })
        .ok_or_else(|| {
            HawDBError::StorageIntegrity("repair path does not identify its branch UUID".into())
        })?;
    let catalog = crate::branch_catalog::read_catalog(&branches.join("catalog.hawdb"))?;
    let record = catalog
        .branches
        .iter()
        .find(|record| record.id.as_uuid() == source.branch_id)
        .ok_or_else(|| {
            HawDBError::StorageIntegrity("repair branch is absent from the catalog".into())
        })?;
    if catalog.project_id.as_uuid() != source.project_id
        || record.metadata_revision != source.metadata_revision
        || record.state != crate::branch_catalog::BranchState::Ready
    {
        return Err(HawDBError::StorageIntegrity(
            "repair branch catalog identity changed".into(),
        ));
    }
    let head = crate::branch_head::read_branch_head(&path.join("branch.head"))
        .map_err(HawDBError::from_storage_error)?;
    if head.project_id != *source.project_id.as_bytes()
        || head.branch_id != *source.branch_id.as_bytes()
    {
        return Err(HawDBError::StorageIntegrity(
            "repair branch head identity changed".into(),
        ));
    }
    let objects = crate::immutable_object::ImmutableObjectStore::open(branches.join("objects"))
        .map_err(HawDBError::from_storage_error)?;
    let root = crate::sealed_root::SealedRoot::decode(
        &objects
            .read(head.sealed_root)
            .map_err(HawDBError::from_storage_error)?,
    )
    .map_err(HawDBError::from_storage_error)?;
    if root.commit_epoch != head.logical_commit_epoch
        || root.replay_end_lsn() != head.active_wal.replay_start_lsn
    {
        return Err(HawDBError::StorageIntegrity(
            "repair branch root does not match its head".into(),
        ));
    }
    objects
        .read(root.durable_manifest)
        .map_err(HawDBError::from_storage_error)?;
    let manifest_path = objects.object_path(root.durable_manifest);
    let wal_path = path.join(crate::artifact_files::wal_generation_file(
        head.active_wal.generation,
    ));
    Ok(PublishedRepairBranch {
        head,
        root,
        objects,
        manifest_path,
        wal_path,
    })
}
