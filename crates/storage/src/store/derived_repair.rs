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
pub use hawdb_storage::{
    DerivedArtifactHealth, DerivedArtifactHealthReport, DerivedArtifactHealthState,
    DerivedArtifactKind, DerivedArtifactRebuildOptions, DerivedArtifactRepairPlan,
    DerivedArtifactRepairReport, DERIVED_ARTIFACT_REPAIR_PROTOCOL,
};
use std::path::Path;
use std::sync::Arc;

#[path = "derived_repair/audit.rs"]
#[doc(hidden)]
pub mod audit;
use audit::{
    finalize_repair, load_matching_pending_record, load_single_pending_record,
    pending_record_paths, prepare_repair, validate_pending_record,
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
        let path = path.as_ref();
        if let Some(record) = load_single_pending_record(path)? {
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
        let path = path.as_ref();
        if let Some(record) = load_single_pending_record(path)? {
            validate_pending_record(path, &record)?;
            return Ok(record.plan);
        }
        let inspection = inspect(path, options)?;
        plan_from_inspection(path, &inspection, options)
    }

    pub fn apply_derived_artifact_rebuild(
        path: impl AsRef<Path>,
        plan: &DerivedArtifactRepairPlan,
    ) -> Result<DerivedArtifactRepairReport> {
        validate_plan(plan)?;
        let path = path.as_ref();
        if let Some(record) = load_matching_pending_record(path, &plan.plan_id)? {
            let inspection = inspect(path, record.plan.options)?;
            if inspection.manifest.checkpoint_epoch == record.plan.target_generation
                && !inspection.health.repair_required
            {
                return finalize_repair(path, record, true);
            }
        }

        let mut inspection = inspect(path, plan.options)?;
        let current_plan = plan_from_inspection(path, &inspection, plan.options)?;
        if current_plan != *plan {
            return Err(HawDBError::Storage(
                "derived artifact repair plan no longer matches the current database state"
                    .to_string(),
            ));
        }
        let prepared = match load_matching_pending_record(path, &plan.plan_id)? {
            Some(record) => record,
            None => prepare_repair(path, plan)?,
        };
        validate_pending_record(path, &prepared)?;
        validate_source_identity(path, plan)?;
        inspection.store.enable_derived_repair_writes()?;
        let build_config = build_config(plan.options)?;
        inspection
            .store
            .checkpoint_with_reader_epoch_and_build_config(
                &inspection.recovered_catalog,
                None,
                build_config,
            )?;
        drop(inspection.store);

        let published = inspect(path, plan.options)?;
        if published.manifest.checkpoint_epoch != plan.target_generation
            || published.manifest.checkpoint_commit_epoch != plan.source_commit_epoch
            || published.health.repair_required
        {
            return Err(HawDBError::Storage(
                "derived artifact rebuild did not publish one healthy target generation; pending audit was retained"
                    .to_string(),
            ));
        }
        drop(published.store);
        finalize_repair(path, prepared, false)
    }
}

pub(super) fn reject_pending_derived_artifact_repair(path: &Path) -> Result<()> {
    let pending = pending_record_paths(path)?;
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
    let (store, recovered_catalog, _) = GraphStore::open_for_derived_repair(path, replay)?;
    let manifest = DurableManifest::load(&path.join(MANIFEST_FILE))?;
    manifest.validate()?;
    let (node_count, relationship_count, logical_bytes) =
        validate_canonical_source(&store, options)?;
    let artifacts = assess_artifacts(path, &store, manifest, options);
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
) -> Vec<DerivedArtifactHealth> {
    if manifest.checkpoint_generation.is_none() {
        return Vec::new();
    }
    let canonical_relationship_count = store
        .canonical_base
        .as_ref()
        .map(|reader| reader.manifest().relationship_count)
        .unwrap_or_default();
    vec![
        assess_one(DerivedArtifactKind::CanonicalAdjacency, || {
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
        }),
        assess_one(DerivedArtifactKind::PersistentPropertyProjection, || {
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
        }),
    ]
}

fn assess_one(
    kind: DerivedArtifactKind,
    check: impl FnOnce() -> Result<()>,
) -> DerivedArtifactHealth {
    match check() {
        Ok(()) => DerivedArtifactHealth {
            kind,
            state: DerivedArtifactHealthState::Healthy,
            reason_code: None,
        },
        Err(_) => DerivedArtifactHealth {
            kind,
            state: DerivedArtifactHealthState::RepairRequired,
            reason_code: Some("artifact_missing_corrupt_or_inconsistent".to_string()),
        },
    }
}

fn plan_from_inspection(
    path: &Path,
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
    let manifest_identity = file_checksum(&path.join(MANIFEST_FILE))?;
    let wal_identity = file_checksum(&inspection.manifest.wal_path(path))?;
    let mut plan = DerivedArtifactRepairPlan {
        protocol: DERIVED_ARTIFACT_REPAIR_PROTOCOL.to_string(),
        plan_id: String::new(),
        source_generation: inspection.manifest.checkpoint_epoch,
        target_generation: inspection.manifest.checkpoint_epoch.saturating_add(1),
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
    let manifest = file_checksum(&path.join(MANIFEST_FILE))?;
    let durable_manifest = DurableManifest::load(&path.join(MANIFEST_FILE))?;
    let wal = file_checksum(&durable_manifest.wal_path(path))?;
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
